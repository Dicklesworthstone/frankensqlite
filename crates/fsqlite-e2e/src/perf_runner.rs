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
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[cfg(test)]
use fsqlite_btree::{BtreeCopyProfileSnapshot, BtreeLeafReuseSnapshot};
use fsqlite_core::connection::{
    HotPathProfileSnapshot, ParserHotPathProfileSnapshot, hot_path_profile_enabled,
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};

use crate::HarnessSettings;
use crate::benchmark::{
    BenchmarkComparisonMetadata, BenchmarkConfig, BenchmarkMeta, BenchmarkSummary, run_benchmark,
};
use crate::fixture_select::{BenchmarkArtifactCommand, BenchmarkArtifactToolVersion};
use crate::fsqlite_executor::run_oplog_fsqlite;
use crate::oplog::{self, OpLog};
use crate::report::{EngineRunReport, HotPathRetryBreakdown, WalHotPathProfile};
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
    /// Error message when the cell is not fully comparable.
    ///
    /// This includes total cell failures and partial measurement runs where at
    /// least one iteration errored.
    pub error: Option<String>,
    /// Engine name.
    pub engine: String,
    /// Canonical comparison mode id when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode_id: Option<String>,
    /// Fixture ID.
    pub fixture_id: String,
    /// Workload preset name.
    pub workload: String,
    /// Concurrency level.
    pub concurrency: u16,
}

impl CellOutcome {
    /// Whether this cell can participate in engine-to-engine comparisons.
    #[must_use]
    pub fn is_fully_comparable(&self) -> bool {
        self.summary.is_some() && self.error.is_none()
    }

    /// Summary data only when every measurement iteration completed cleanly.
    #[must_use]
    pub fn comparable_summary(&self) -> Option<&BenchmarkSummary> {
        self.summary.as_ref().filter(|_| self.error.is_none())
    }
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
/// Schema version for hot-path profile reports.
pub const HOT_PATH_PROFILE_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_profile.v1";
/// Schema version for hot-path artifact manifests.
pub const HOT_PATH_PROFILE_MANIFEST_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_profile_manifest.v1";
/// Schema version for raw opcode profile packs.
pub const HOT_PATH_OPCODE_PROFILE_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_opcode_profile.v1";
/// Schema version for raw subsystem profile packs.
pub const HOT_PATH_SUBSYSTEM_PROFILE_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_subsystem_profile.v1";
/// Schema version for structured actionable hotspot and baseline-tax artifacts.
pub const HOT_PATH_PROFILE_ACTIONABLE_RANKING_SCHEMA_V3: &str =
    "fsqlite-e2e.hot_path_actionable_ranking.v3";
/// Bead identifier for the hot-path profiling work.
pub const HOT_PATH_PROFILE_BEAD_ID: &str = "bd-db300.4.1";
/// Canonical scenario identifier prefix for preset-specific hot-path profiles.
pub const HOT_PATH_PROFILE_SCENARIO_PREFIX: &str = HOT_PATH_PROFILE_BEAD_ID;

/// Configuration for a focused FrankenSQLite hot-path profile.
#[derive(Debug, Clone)]
pub struct FsqliteHotPathProfileConfig {
    pub workload: String,
    pub seed: u64,
    pub scale: u32,
    pub concurrency: u16,
    pub exec_config: crate::fsqlite_executor::FsqliteExecConfig,
    pub replay_command: String,
    pub golden_dir: Option<String>,
    pub working_base: Option<String>,
    pub bead_id: Option<String>,
    pub scenario_prefix: Option<String>,
}

impl FsqliteHotPathProfileConfig {
    fn bead_id(&self) -> &str {
        self.bead_id.as_deref().unwrap_or(HOT_PATH_PROFILE_BEAD_ID)
    }

    fn scenario_prefix(&self) -> &str {
        self.scenario_prefix
            .as_deref()
            .unwrap_or_else(|| self.bead_id())
    }
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
    pub prepared_cache_hits: u64,
    pub prepared_cache_misses: u64,
    pub statement_cache_hits: u64,
    pub statement_cache_misses: u64,
    pub plan_cache_hits: u64,
    pub plan_cache_misses: u64,
    pub compile_reuse_count: u64,
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
    pub decode_cache_hits_total: u64,
    pub decode_cache_misses_total: u64,
    pub record_decodes_per_row_basis_points: Option<u32>,
    pub decode_cache_invalidations_position_total: u64,
    pub decode_cache_invalidations_write_total: u64,
    pub decode_cache_invalidations_pseudo_total: u64,
    pub callsite_breakdown: HotPathRecordDecodeCallsiteBreakdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HotPathRecordDecodeCallsiteCounters {
    pub parse_record_calls: u64,
    pub parse_record_into_calls: u64,
    pub parse_record_column_calls: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HotPathRecordDecodeCallsiteBreakdown {
    pub unattributed: HotPathRecordDecodeCallsiteCounters,
    pub core_connection: HotPathRecordDecodeCallsiteCounters,
    pub core_compat_persist: HotPathRecordDecodeCallsiteCounters,
    pub vdbe_engine: HotPathRecordDecodeCallsiteCounters,
    pub vdbe_vectorized_scan: HotPathRecordDecodeCallsiteCounters,
    pub btree_cursor: HotPathRecordDecodeCallsiteCounters,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathBtreeCopyKernelProfile {
    pub local_payload_copy_calls: u64,
    pub local_payload_copy_bytes: u64,
    pub owned_payload_materialization_calls: u64,
    pub owned_payload_materialization_bytes: u64,
    pub overflow_chain_reassembly_calls: u64,
    pub overflow_chain_local_bytes: u64,
    pub overflow_chain_overflow_bytes: u64,
    pub overflow_page_reads: u64,
    pub table_leaf_cell_assembly_calls: u64,
    pub table_leaf_cell_assembly_bytes: u64,
    pub index_leaf_cell_assembly_calls: u64,
    pub index_leaf_cell_assembly_bytes: u64,
    pub interior_cell_rebuild_calls: u64,
    pub interior_cell_rebuild_bytes: u64,
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
pub struct HotPathMvccWriteProfile {
    pub total_write_attempts: u64,
    pub tier0_already_owned_writes_total: u64,
    pub tier1_first_touch_writes_total: u64,
    pub tier2_commit_surface_writes_total: u64,
    pub page_lock_waits_total: u64,
    pub page_lock_wait_time_ns_total: u64,
    pub write_busy_retries_total: u64,
    pub write_busy_timeouts_total: u64,
    pub stale_snapshot_rejects_total: u64,
    pub page_one_conflict_tracks_total: u64,
    pub page_one_conflict_track_time_ns_total: u64,
    pub pending_commit_surface_clears_total: u64,
    pub pending_commit_surface_clear_time_ns_total: u64,
    pub runtime_retry: HotPathRetryBreakdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathPageDataMotionProfile {
    pub borrowed_write_normalization_calls_total: u64,
    pub borrowed_exact_size_copies_total: u64,
    pub owned_write_normalization_calls_total: u64,
    pub owned_passthrough_total: u64,
    pub owned_in_place_zero_extends_total: u64,
    pub owned_resized_copies_total: u64,
    pub normalized_payload_bytes_total: u64,
    pub normalized_zero_fill_bytes_total: u64,
    pub normalized_bytes_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathPageBufferPoolProfile {
    pub page_buffer_pool_hits: u64,
    pub page_buffer_pool_misses: u64,
    pub page_image_slab_reuse_count: u64,
    pub staged_page_copy_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathConnectionCeremonyProfile {
    pub background_status_time_ns: u64,
    pub background_status_checks: u64,
    pub op_cx_background_gates: u64,
    pub statement_dispatch_background_gates: u64,
    pub prepared_lookup_time_ns: u64,
    pub prepared_schema_refresh_time_ns: u64,
    pub prepared_schema_refreshes: u64,
    pub prepared_schema_lightweight_refreshes: u64,
    pub prepared_schema_full_reloads: u64,
    pub pager_publication_refreshes: u64,
    pub memory_autocommit_fast_path_begins: u64,
    pub cached_read_snapshot_reuses: u64,
    pub cached_read_snapshot_parks: u64,
    pub begin_setup_time_ns: u64,
    pub begin_refresh_count: u64,
    pub commit_refresh_count: u64,
    pub memdb_refresh_count: u64,
    pub execute_body_time_ns: u64,
    pub direct_write_flush_calls: u64,
    pub direct_write_flush_time_ns: u64,
    pub commit_pre_txn_time_ns: u64,
    pub commit_txn_roundtrip_time_ns: u64,
    pub commit_finalize_seq_time_ns: u64,
    pub commit_handle_finalize_time_ns: u64,
    pub commit_post_write_maintenance_time_ns: u64,
    pub finalize_post_publish_time_ns: u64,
    pub column_default_evaluation_passes: u64,
    pub prepared_table_engine_fresh_allocs: u64,
    pub prepared_table_engine_reuses: u64,
    pub prepared_insert_fast_lane_hits: u64,
    pub prepared_insert_instrumented_lane_hits: u64,
    pub prepared_update_delete_fast_lane_hits: u64,
    pub prepared_update_delete_instrumented_lane_hits: u64,
    pub prepared_update_delete_fallback_returning: u64,
    pub prepared_update_delete_fallback_sqlite_sequence: u64,
    pub prepared_update_delete_fallback_without_rowid: u64,
    pub prepared_update_delete_fallback_live_vtab: u64,
    pub prepared_update_delete_fallback_trigger: u64,
    pub prepared_update_delete_fallback_foreign_key: u64,
    pub prepared_table_dml_affected_only_runs: u64,
    pub autoincrement_sequence_fast_path_updates: u64,
    pub autoincrement_sequence_scan_refreshes: u64,
    pub arena_alloc_bytes: u64,
    pub arena_reset_count: u64,
    pub fallback_to_general_allocator_count: u64,
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
    pub btree_copy_kernels: HotPathBtreeCopyKernelProfile,
    pub btree_copy_kernel_targets: Vec<HotPathRankingEntry>,
    pub record_decode: HotPathRecordDecodeProfile,
    pub row_materialization: HotPathRowMaterializationProfile,
    #[serde(default)]
    pub wal: WalHotPathProfile,
    pub mvcc_write: HotPathMvccWriteProfile,
    pub page_buffer_pool: HotPathPageBufferPoolProfile,
    pub page_data_motion: HotPathPageDataMotionProfile,
    pub connection_ceremony: HotPathConnectionCeremonyProfile,
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
pub struct HotPathBaselineReuseLedgerEntry {
    pub rank: u32,
    pub surface: String,
    pub supported: bool,
    pub hits: u64,
    pub misses: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hit_rate_basis_points: Option<u32>,
    pub rationale: String,
    pub implication: String,
    pub mapped_beads: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathBaselineWasteLedgerEntry {
    pub rank: u32,
    pub component: String,
    pub classification: String,
    pub metric_kind: String,
    pub metric_value: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_share_basis_points: Option<u32>,
    pub allocator_pressure_bytes: u64,
    pub activity_count: u64,
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
pub struct HotPathWallTimeComponentEntry {
    pub rank: u32,
    pub component: String,
    pub time_ns: u64,
    pub wall_share_basis_points: u32,
    pub rationale: String,
    pub implication: String,
    pub mapped_beads: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathCausalEvidence {
    pub artifact: String,
    pub metric_path: String,
    pub metric_kind: String,
    pub metric_value: u64,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathCausalBucketEntry {
    pub rank: u32,
    pub bucket: String,
    pub dominant: bool,
    pub estimated_time_ns: u64,
    pub wall_share_basis_points: u32,
    pub score_basis_points: u32,
    pub rationale: String,
    pub implication: String,
    pub mapped_beads: Vec<String>,
    pub evidence: Vec<HotPathCausalEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathCausalClassificationSummary {
    pub dominant_bucket: String,
    pub dominant_estimated_time_ns: u64,
    pub dominant_wall_share_basis_points: u32,
    pub dominant_score_basis_points: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_up_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_up_estimated_time_ns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_up_score_basis_points: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_up_gap_basis_points: Option<u32>,
    pub mixed_or_ambiguous: bool,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathMicroarchitecturalSignatureEntry {
    pub rank: u32,
    pub target: String,
    pub primary_signature: String,
    pub secondary_signatures: Vec<String>,
    pub confidence_label: String,
    pub confidence_score_basis_points: u32,
    pub mixed_or_ambiguous: bool,
    pub rationale: String,
    pub evidence_sources: Vec<String>,
    pub fixture_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardware_class_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardware_signature: Option<String>,
    pub implication: String,
    pub mapped_beads: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathMicroarchitecturalContext {
    pub fixture_id: String,
    pub row_id: String,
    pub mode_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardware_class_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardware_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathAllocatorPressure {
    pub parser_sql_bytes: u64,
    pub decoded_value_heap_bytes_total: u64,
    pub result_value_heap_bytes_total: u64,
    pub record_vec_capacity_slots: u64,
    pub page_data_normalization_bytes_total: u64,
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
    pub btree_copy_kernels: HotPathBtreeCopyKernelProfile,
    pub btree_copy_kernel_targets: Vec<HotPathRankingEntry>,
    pub record_decode: HotPathRecordDecodeProfile,
    pub row_materialization: HotPathRowMaterializationProfile,
    pub mvcc_write: HotPathMvccWriteProfile,
    pub page_buffer_pool: HotPathPageBufferPoolProfile,
    pub page_data_motion: HotPathPageDataMotionProfile,
    pub connection_ceremony: HotPathConnectionCeremonyProfile,
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
    pub baseline_reuse_ledger: Vec<HotPathBaselineReuseLedgerEntry>,
    pub baseline_waste_ledger: Vec<HotPathBaselineWasteLedgerEntry>,
    pub named_hotspots: Vec<HotPathActionableRankingEntry>,
    pub microarchitectural_signatures: Vec<HotPathMicroarchitecturalSignatureEntry>,
    pub wall_time_components: Vec<HotPathWallTimeComponentEntry>,
    pub causal_classification: HotPathCausalClassificationSummary,
    pub causal_buckets: Vec<HotPathCausalBucketEntry>,
    pub cost_components: Vec<HotPathCostComponentEntry>,
    pub allocator_pressure: Vec<HotPathActionableRankingEntry>,
    pub top_opcodes: Vec<HotPathOpcodeProfileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathArtifactFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathCounterCaptureManifestSummary {
    pub host_capability_sensitive_captures: Vec<String>,
    pub topology_sensitive_captures: Vec<String>,
    pub fallback_tools: Vec<String>,
    pub fallback_metric_pack: Vec<String>,
    pub fallback_notes: Vec<String>,
    pub raw_output_relpaths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathArtifactProvenance {
    pub row_id: String,
    pub mode_id: String,
    pub artifact_root: String,
    pub command_entrypoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub beads_data_hash: Option<String>,
    pub kernel_release: String,
    pub rustc_version: String,
    pub cargo_profile: String,
    pub commands: Vec<BenchmarkArtifactCommand>,
    pub tool_versions: Vec<BenchmarkArtifactToolVersion>,
    pub fallback_notes: Vec<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counter_capture_summary: Option<HotPathCounterCaptureManifestSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<HotPathArtifactProvenance>,
    pub files: Vec<HotPathArtifactFile>,
}

static HOT_PATH_PROFILE_SCOPE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
#[cfg(test)]
pub(crate) static HOT_PATH_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct HotPathProfileScope {
    was_enabled: bool,
    _guard: MutexGuard<'static, ()>,
}

impl HotPathProfileScope {
    fn enable() -> crate::E2eResult<Self> {
        let guard = HOT_PATH_PROFILE_SCOPE_LOCK.try_lock().map_err(|_| {
            crate::E2eError::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "hot-path profiling is already active in this process",
            ))
        })?;
        let was_enabled = hot_path_profile_enabled();
        if was_enabled {
            return Err(crate::E2eError::Io(std::io::Error::other(
                "hot-path profiling was already enabled before scope entry",
            )));
        }
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        Ok(Self {
            was_enabled,
            _guard: guard,
        })
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
        prepared_cache_hits: snapshot.prepared_cache_hits,
        prepared_cache_misses: snapshot.prepared_cache_misses,
        statement_cache_hits: snapshot.prepared_cache_hits,
        statement_cache_misses: snapshot.prepared_cache_misses,
        plan_cache_hits: snapshot.compiled_cache_hits,
        plan_cache_misses: snapshot.compiled_cache_misses,
        compile_reuse_count: snapshot
            .compiled_cache_hits
            .saturating_add(snapshot.prepared_cache_hits),
        compile_time_ns: snapshot.compile_time_ns,
    }
}

fn btree_copy_kernel_targets(profile: &HotPathBtreeCopyKernelProfile) -> Vec<HotPathRankingEntry> {
    let mut entries = vec![
        HotPathRankingEntry {
            subsystem: "btree_local_payload_copy".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: profile.local_payload_copy_bytes,
            rationale: format!(
                "{} local payload copy call(s) copied {} byte(s) into caller scratch without overflow traversal",
                profile.local_payload_copy_calls, profile.local_payload_copy_bytes
            ),
        },
        HotPathRankingEntry {
            subsystem: "btree_owned_payload_materialization".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: profile.owned_payload_materialization_bytes,
            rationale: format!(
                "{} payload materialization call(s) forced {} byte(s) into fresh owned buffers",
                profile.owned_payload_materialization_calls,
                profile.owned_payload_materialization_bytes
            ),
        },
        HotPathRankingEntry {
            subsystem: "btree_overflow_reassembly".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: profile
                .overflow_chain_local_bytes
                .saturating_add(profile.overflow_chain_overflow_bytes),
            rationale: format!(
                "{} overflow reassembly call(s) copied {} local byte(s) + {} overflow byte(s) across {} overflow page read(s)",
                profile.overflow_chain_reassembly_calls,
                profile.overflow_chain_local_bytes,
                profile.overflow_chain_overflow_bytes,
                profile.overflow_page_reads
            ),
        },
        HotPathRankingEntry {
            subsystem: "btree_table_leaf_cell_assembly".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: profile.table_leaf_cell_assembly_bytes,
            rationale: format!(
                "{} table-leaf cell assembly call(s) emitted {} byte(s) before page insert",
                profile.table_leaf_cell_assembly_calls, profile.table_leaf_cell_assembly_bytes
            ),
        },
        HotPathRankingEntry {
            subsystem: "btree_index_leaf_cell_assembly".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: profile.index_leaf_cell_assembly_bytes,
            rationale: format!(
                "{} index-leaf cell assembly call(s) emitted {} byte(s) before page insert",
                profile.index_leaf_cell_assembly_calls, profile.index_leaf_cell_assembly_bytes
            ),
        },
        HotPathRankingEntry {
            subsystem: "btree_interior_cell_rebuild".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: profile.interior_cell_rebuild_bytes,
            rationale: format!(
                "{} interior-cell rebuild call(s) emitted {} byte(s) while replacing separator cells",
                profile.interior_cell_rebuild_calls, profile.interior_cell_rebuild_bytes
            ),
        },
    ];
    entries.retain(|entry| entry.metric_value > 0);
    entries.sort_by(|lhs, rhs| {
        rhs.metric_value
            .cmp(&lhs.metric_value)
            .then_with(|| lhs.subsystem.cmp(&rhs.subsystem))
    });
    entries
}

#[must_use]
fn record_decode_callsite_counters(
    parse_record_calls: u64,
    parse_record_into_calls: u64,
    parse_record_column_calls: u64,
) -> HotPathRecordDecodeCallsiteCounters {
    HotPathRecordDecodeCallsiteCounters {
        parse_record_calls,
        parse_record_into_calls,
        parse_record_column_calls,
    }
}

#[must_use]
fn hottest_full_record_decode_callsite(
    breakdown: &HotPathRecordDecodeCallsiteBreakdown,
) -> Option<(&'static str, u64)> {
    [
        ("btree_cursor", breakdown.btree_cursor.parse_record_calls),
        ("vdbe_engine", breakdown.vdbe_engine.parse_record_calls),
        (
            "core_connection",
            breakdown.core_connection.parse_record_calls,
        ),
        (
            "core_compat_persist",
            breakdown.core_compat_persist.parse_record_calls,
        ),
        (
            "vdbe_vectorized_scan",
            breakdown.vdbe_vectorized_scan.parse_record_calls,
        ),
        ("unattributed", breakdown.unattributed.parse_record_calls),
    ]
    .into_iter()
    .max_by(|lhs, rhs| lhs.1.cmp(&rhs.1).then_with(|| lhs.0.cmp(rhs.0)))
    .filter(|(_, total)| *total > 0)
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
    let btree_copy_kernels = HotPathBtreeCopyKernelProfile {
        local_payload_copy_calls: snapshot.btree_copy_kernels.local_payload_copy_calls,
        local_payload_copy_bytes: snapshot.btree_copy_kernels.local_payload_copy_bytes,
        owned_payload_materialization_calls: snapshot
            .btree_copy_kernels
            .owned_payload_materialization_calls,
        owned_payload_materialization_bytes: snapshot
            .btree_copy_kernels
            .owned_payload_materialization_bytes,
        overflow_chain_reassembly_calls: snapshot
            .btree_copy_kernels
            .overflow_chain_reassembly_calls,
        overflow_chain_local_bytes: snapshot.btree_copy_kernels.overflow_chain_local_bytes,
        overflow_chain_overflow_bytes: snapshot.btree_copy_kernels.overflow_chain_overflow_bytes,
        overflow_page_reads: snapshot.btree_copy_kernels.overflow_page_reads,
        table_leaf_cell_assembly_calls: snapshot.btree_copy_kernels.table_leaf_cell_assembly_calls,
        table_leaf_cell_assembly_bytes: snapshot.btree_copy_kernels.table_leaf_cell_assembly_bytes,
        index_leaf_cell_assembly_calls: snapshot.btree_copy_kernels.index_leaf_cell_assembly_calls,
        index_leaf_cell_assembly_bytes: snapshot.btree_copy_kernels.index_leaf_cell_assembly_bytes,
        interior_cell_rebuild_calls: snapshot.btree_copy_kernels.interior_cell_rebuild_calls,
        interior_cell_rebuild_bytes: snapshot.btree_copy_kernels.interior_cell_rebuild_bytes,
    };
    let btree_copy_kernel_targets = btree_copy_kernel_targets(&btree_copy_kernels);
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
        decode_cache_hits_total: snapshot.vdbe.decode_cache_hits_total,
        decode_cache_misses_total: snapshot.vdbe.decode_cache_misses_total,
        record_decodes_per_row_basis_points: (snapshot.vdbe.result_rows_total != 0).then(|| {
            ratio_basis_points(
                snapshot.vdbe.decode_cache_misses_total,
                snapshot.vdbe.result_rows_total,
            )
        }),
        decode_cache_invalidations_position_total: snapshot
            .vdbe
            .decode_cache_invalidations_position_total,
        decode_cache_invalidations_write_total: snapshot
            .vdbe
            .decode_cache_invalidations_write_total,
        decode_cache_invalidations_pseudo_total: snapshot
            .vdbe
            .decode_cache_invalidations_pseudo_total,
        callsite_breakdown: HotPathRecordDecodeCallsiteBreakdown {
            unattributed: record_decode_callsite_counters(
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .unattributed
                    .parse_record_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .unattributed
                    .parse_record_into_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .unattributed
                    .parse_record_column_calls,
            ),
            core_connection: record_decode_callsite_counters(
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .core_connection
                    .parse_record_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .core_connection
                    .parse_record_into_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .core_connection
                    .parse_record_column_calls,
            ),
            core_compat_persist: record_decode_callsite_counters(
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .core_compat_persist
                    .parse_record_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .core_compat_persist
                    .parse_record_into_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .core_compat_persist
                    .parse_record_column_calls,
            ),
            vdbe_engine: record_decode_callsite_counters(
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .vdbe_engine
                    .parse_record_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .vdbe_engine
                    .parse_record_into_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .vdbe_engine
                    .parse_record_column_calls,
            ),
            vdbe_vectorized_scan: record_decode_callsite_counters(
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .vdbe_vectorized_scan
                    .parse_record_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .vdbe_vectorized_scan
                    .parse_record_into_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .vdbe_vectorized_scan
                    .parse_record_column_calls,
            ),
            btree_cursor: record_decode_callsite_counters(
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .btree_cursor
                    .parse_record_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .btree_cursor
                    .parse_record_into_calls,
                snapshot
                    .record_decode
                    .callsite_breakdown
                    .btree_cursor
                    .parse_record_column_calls,
            ),
        },
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
    let runtime_retry = engine_report
        .hot_path_profile
        .as_ref()
        .map(|profile| profile.runtime_retry.clone())
        .unwrap_or_else(|| HotPathRetryBreakdown {
            total_retries: engine_report.retries,
            total_aborts: engine_report.aborts,
            ..HotPathRetryBreakdown::default()
        });
    let mvcc_write = HotPathMvccWriteProfile {
        total_write_attempts: snapshot
            .vdbe
            .mvcc_write_path
            .tier0_already_owned_writes_total
            .saturating_add(snapshot.vdbe.mvcc_write_path.tier1_first_touch_writes_total)
            .saturating_add(
                snapshot
                    .vdbe
                    .mvcc_write_path
                    .tier2_commit_surface_writes_total,
            ),
        tier0_already_owned_writes_total: snapshot
            .vdbe
            .mvcc_write_path
            .tier0_already_owned_writes_total,
        tier1_first_touch_writes_total: snapshot
            .vdbe
            .mvcc_write_path
            .tier1_first_touch_writes_total,
        tier2_commit_surface_writes_total: snapshot
            .vdbe
            .mvcc_write_path
            .tier2_commit_surface_writes_total,
        page_lock_waits_total: snapshot.vdbe.mvcc_write_path.page_lock_waits_total,
        page_lock_wait_time_ns_total: snapshot.vdbe.mvcc_write_path.page_lock_wait_time_ns_total,
        write_busy_retries_total: snapshot.vdbe.mvcc_write_path.write_busy_retries_total,
        write_busy_timeouts_total: snapshot.vdbe.mvcc_write_path.write_busy_timeouts_total,
        stale_snapshot_rejects_total: snapshot.vdbe.mvcc_write_path.stale_snapshot_rejects_total,
        page_one_conflict_tracks_total: snapshot
            .vdbe
            .mvcc_write_path
            .page_one_conflict_tracks_total,
        page_one_conflict_track_time_ns_total: snapshot
            .vdbe
            .mvcc_write_path
            .page_one_conflict_track_time_ns_total,
        pending_commit_surface_clears_total: snapshot
            .vdbe
            .mvcc_write_path
            .pending_commit_surface_clears_total,
        pending_commit_surface_clear_time_ns_total: snapshot
            .vdbe
            .mvcc_write_path
            .pending_commit_surface_clear_time_ns_total,
        runtime_retry,
    };
    let page_data_normalization_bytes_total = snapshot
        .vdbe
        .page_data_motion
        .normalized_payload_bytes_total
        .saturating_add(
            snapshot
                .vdbe
                .page_data_motion
                .normalized_zero_fill_bytes_total,
        );
    let page_data_motion = HotPathPageDataMotionProfile {
        borrowed_write_normalization_calls_total: snapshot
            .vdbe
            .page_data_motion
            .borrowed_write_normalization_calls_total,
        borrowed_exact_size_copies_total: snapshot
            .vdbe
            .page_data_motion
            .borrowed_exact_size_copies_total,
        owned_write_normalization_calls_total: snapshot
            .vdbe
            .page_data_motion
            .owned_write_normalization_calls_total,
        owned_passthrough_total: snapshot.vdbe.page_data_motion.owned_passthrough_total,
        owned_in_place_zero_extends_total: snapshot
            .vdbe
            .page_data_motion
            .owned_in_place_zero_extends_total,
        owned_resized_copies_total: snapshot.vdbe.page_data_motion.owned_resized_copies_total,
        normalized_payload_bytes_total: snapshot
            .vdbe
            .page_data_motion
            .normalized_payload_bytes_total,
        normalized_zero_fill_bytes_total: snapshot
            .vdbe
            .page_data_motion
            .normalized_zero_fill_bytes_total,
        normalized_bytes_total: page_data_normalization_bytes_total,
    };
    let page_buffer_pool = HotPathPageBufferPoolProfile {
        page_buffer_pool_hits: snapshot.page_buffer_pool_hits,
        page_buffer_pool_misses: snapshot.page_buffer_pool_misses,
        page_image_slab_reuse_count: snapshot.page_buffer_pool_hits,
        staged_page_copy_bytes: page_data_normalization_bytes_total,
    };
    let connection_ceremony = HotPathConnectionCeremonyProfile {
        background_status_time_ns: snapshot.background_status_time_ns,
        background_status_checks: snapshot.background_status_checks,
        op_cx_background_gates: snapshot.op_cx_background_gates,
        statement_dispatch_background_gates: snapshot.statement_dispatch_background_gates,
        prepared_lookup_time_ns: snapshot.prepared_lookup_time_ns,
        prepared_schema_refresh_time_ns: snapshot.prepared_schema_refresh_time_ns,
        prepared_schema_refreshes: snapshot.prepared_schema_refreshes,
        prepared_schema_lightweight_refreshes: snapshot.prepared_schema_lightweight_refreshes,
        prepared_schema_full_reloads: snapshot.prepared_schema_full_reloads,
        pager_publication_refreshes: snapshot.pager_publication_refreshes,
        memory_autocommit_fast_path_begins: snapshot.memory_autocommit_fast_path_begins,
        cached_read_snapshot_reuses: snapshot.cached_read_snapshot_reuses,
        cached_read_snapshot_parks: snapshot.cached_read_snapshot_parks,
        begin_setup_time_ns: snapshot.begin_setup_time_ns,
        begin_refresh_count: snapshot.begin_refresh_count,
        commit_refresh_count: snapshot.commit_refresh_count,
        memdb_refresh_count: snapshot.memdb_refresh_count,
        execute_body_time_ns: snapshot.execute_body_time_ns,
        direct_write_flush_calls: snapshot.direct_write_flush_calls,
        direct_write_flush_time_ns: snapshot.direct_write_flush_time_ns,
        commit_pre_txn_time_ns: snapshot.commit_pre_txn_time_ns,
        commit_txn_roundtrip_time_ns: snapshot.commit_txn_roundtrip_time_ns,
        commit_finalize_seq_time_ns: snapshot.commit_finalize_seq_time_ns,
        commit_handle_finalize_time_ns: snapshot.commit_handle_finalize_time_ns,
        commit_post_write_maintenance_time_ns: snapshot.commit_post_write_maintenance_time_ns,
        finalize_post_publish_time_ns: snapshot.finalize_post_publish_time_ns,
        column_default_evaluation_passes: snapshot.column_default_evaluation_passes,
        prepared_table_engine_fresh_allocs: snapshot.prepared_table_engine_fresh_allocs,
        prepared_table_engine_reuses: snapshot.prepared_table_engine_reuses,
        prepared_insert_fast_lane_hits: snapshot.prepared_insert_fast_lane_hits,
        prepared_insert_instrumented_lane_hits: snapshot.prepared_insert_instrumented_lane_hits,
        prepared_update_delete_fast_lane_hits: snapshot.prepared_update_delete_fast_lane_hits,
        prepared_update_delete_instrumented_lane_hits: snapshot
            .prepared_update_delete_instrumented_lane_hits,
        prepared_update_delete_fallback_returning: snapshot
            .prepared_update_delete_fallback_returning,
        prepared_update_delete_fallback_sqlite_sequence: snapshot
            .prepared_update_delete_fallback_sqlite_sequence,
        prepared_update_delete_fallback_without_rowid: snapshot
            .prepared_update_delete_fallback_without_rowid,
        prepared_update_delete_fallback_live_vtab: snapshot
            .prepared_update_delete_fallback_live_vtab,
        prepared_update_delete_fallback_trigger: snapshot.prepared_update_delete_fallback_trigger,
        prepared_update_delete_fallback_foreign_key: snapshot
            .prepared_update_delete_fallback_foreign_key,
        prepared_table_dml_affected_only_runs: snapshot.prepared_table_dml_affected_only_runs,
        autoincrement_sequence_fast_path_updates: snapshot.autoincrement_sequence_fast_path_updates,
        autoincrement_sequence_scan_refreshes: snapshot.autoincrement_sequence_scan_refreshes,
        arena_alloc_bytes: snapshot.arena_alloc_bytes,
        arena_reset_count: snapshot.arena_reset_count,
        fallback_to_general_allocator_count: snapshot.fallback_to_general_allocator_count,
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
        HotPathRankingEntry {
            subsystem: "mvcc_page_lock_wait".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: mvcc_write.page_lock_wait_time_ns_total,
            rationale: "time spent waiting for MVCC page-lock ownership changes".to_owned(),
        },
        HotPathRankingEntry {
            subsystem: "mvcc_page_one_conflict_tracking".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: mvcc_write.page_one_conflict_track_time_ns_total,
            rationale:
                "time spent recording conflict-only page-one tracking on the MVCC write surface"
                    .to_owned(),
        },
        HotPathRankingEntry {
            subsystem: "mvcc_pending_commit_surface_clear".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: mvcc_write.pending_commit_surface_clear_time_ns_total,
            rationale:
                "time spent clearing stale synthetic pending-commit-surface state after writes"
                    .to_owned(),
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
        HotPathRankingEntry {
            subsystem: "page_data_normalization_bytes".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: page_data_normalization_bytes_total,
            rationale:
                "payload-copy + zero-fill bytes materialized while normalizing page images before writes"
                    .to_owned(),
        },
    ];
    allocator_ranking.sort_by(|lhs, rhs| {
        rhs.metric_value
            .cmp(&lhs.metric_value)
            .then_with(|| lhs.subsystem.cmp(&rhs.subsystem))
    });

    HotPathProfileReport {
        schema_version: HOT_PATH_PROFILE_SCHEMA_V1.to_owned(),
        bead_id: config.bead_id().to_owned(),
        scenario_id: hot_path_profile_scenario_id(config.scenario_prefix(), &config.workload),
        run_id,
        trace_id,
        fixture_id: fixture_id.to_owned(),
        workload: config.workload.clone(),
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
        btree_copy_kernels,
        btree_copy_kernel_targets,
        record_decode,
        row_materialization,
        mvcc_write,
        page_buffer_pool,
        page_data_motion,
        connection_ceremony,
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
            page_data_normalization_bytes_total,
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

fn hot_path_profile_scenario_id(scenario_prefix: &str, workload: &str) -> String {
    format!("{scenario_prefix}.{workload}")
}

pub fn profile_fsqlite_hot_path(
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
        "{}-{}-{fixture_id}-c{}-s{}-{now_ms}",
        config.bead_id(),
        config.workload,
        config.concurrency,
        config.seed
    );
    let scenario_id = hot_path_profile_scenario_id(config.scenario_prefix(), &config.workload);
    let trace_id = format!("{scenario_id}:{fixture_id}:c{}", config.concurrency);
    let oplog = generate_oplog(
        &config.workload,
        fixture_id,
        config.seed,
        config.concurrency,
        config.scale,
    )
    .ok_or_else(|| {
        crate::E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("hot-path profile preset unavailable: {}", config.workload),
        ))
    })?;

    let _scope = HotPathProfileScope::enable()?;
    let engine_report = run_oplog_fsqlite(db_path, &oplog, &config.exec_config)?;
    let snapshot = hot_path_profile_snapshot();
    Ok(build_hot_path_profile_report_from_engine_run(
        fixture_id,
        config,
        run_id,
        trace_id,
        engine_report,
        snapshot,
    ))
}

pub fn profile_fsqlite_hot_path_oplog(
    db_path: &Path,
    fixture_id: &str,
    oplog: &crate::oplog::OpLog,
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
        "{}-{}-{fixture_id}-c{}-s{}-{now_ms}",
        config.bead_id(),
        config.workload,
        config.concurrency,
        config.seed
    );
    let trace_id = format!(
        "{}:{fixture_id}:c{}",
        hot_path_profile_scenario_id(config.scenario_prefix(), &config.workload),
        config.concurrency
    );

    let _scope = HotPathProfileScope::enable()?;
    let engine_report = run_oplog_fsqlite(db_path, oplog, &config.exec_config)?;
    let snapshot = hot_path_profile_snapshot();
    Ok(build_hot_path_profile_report_from_engine_run(
        fixture_id,
        config,
        run_id,
        trace_id,
        engine_report,
        snapshot,
    ))
}

fn build_hot_path_profile_report_from_engine_run(
    fixture_id: &str,
    config: &FsqliteHotPathProfileConfig,
    run_id: String,
    trace_id: String,
    engine_report: EngineRunReport,
    snapshot: HotPathProfileSnapshot,
) -> HotPathProfileReport {
    build_hot_path_profile_report(
        fixture_id,
        config,
        run_id,
        trace_id,
        engine_report,
        snapshot,
    )
}

#[must_use]
pub fn render_hot_path_profile_markdown(report: &HotPathProfileReport) -> String {
    let actionable_ranking = build_hot_path_actionable_ranking(report, None, None);
    let mut out = String::with_capacity(4096);
    let _ = writeln!(out, "# Hot-Path Profile\n");
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
    if let Some(notes) = &report.engine_report.correctness.notes {
        let _ = writeln!(out, "- Notes: `{notes}`");
    }
    if let Some(error) = &report.engine_report.error {
        let _ = writeln!(out, "- Error: `{error}`");
    }
    if let Some(diagnostic) = &report.engine_report.first_failure_diagnostic {
        let _ = writeln!(out, "- First failure diagnostic: `{diagnostic}`");
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Parser Reuse\n");
    let _ = writeln!(
        out,
        "- Parse cache hits/misses: {}/{}",
        report.parser.parse_cache_hits, report.parser.parse_cache_misses
    );
    let _ = writeln!(
        out,
        "- `plan_cache_hit` / `plan_cache_miss`: {}/{}",
        report.parser.plan_cache_hits, report.parser.plan_cache_misses
    );
    let _ = writeln!(
        out,
        "- `statement_cache_hit` / `statement_cache_miss`: {}/{}",
        report.parser.statement_cache_hits, report.parser.statement_cache_misses
    );
    let _ = writeln!(
        out,
        "- `compile_reuse_count`: {}",
        report.parser.compile_reuse_count
    );
    let _ = writeln!(
        out,
        "- `invalidation_reason`: emitted by `fsqlite.statement_reuse` telemetry for cache invalidations such as `schema_cookie_changed` and `explicit_invalidate`."
    );
    let _ = writeln!(out);

    let record_decodes_per_row = report
        .record_decode
        .record_decodes_per_row_basis_points
        .map_or_else(|| "n/a".to_owned(), format_basis_points_ratio);
    let _ = writeln!(out, "## Record Decode\n");
    let _ = writeln!(
        out,
        "- `decode_cache_hit` / `decode_cache_miss`: {}/{}",
        report.record_decode.decode_cache_hits_total,
        report.record_decode.decode_cache_misses_total
    );
    let _ = writeln!(
        out,
        "- `decode_invalidation_reason` counts: position={} write={} pseudo={}",
        report
            .record_decode
            .decode_cache_invalidations_position_total,
        report.record_decode.decode_cache_invalidations_write_total,
        report.record_decode.decode_cache_invalidations_pseudo_total
    );
    let _ = writeln!(
        out,
        "- `record_decodes_per_row`: {}",
        record_decodes_per_row
    );
    let _ = writeln!(
        out,
        "- Decode time / heap bytes / column reads: {}/{}/{}",
        report.record_decode.decode_time_ns,
        report.record_decode.vdbe_decoded_value_heap_bytes_total,
        report.record_decode.vdbe_column_reads_total
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "## Connection Ceremony\n");
    let _ = writeln!(
        out,
        "- Background gates: status_checks={} op_cx={} dispatch={}",
        report.connection_ceremony.background_status_checks,
        report.connection_ceremony.op_cx_background_gates,
        report
            .connection_ceremony
            .statement_dispatch_background_gates
    );
    let _ = writeln!(
        out,
        "- Schema/publication refreshes: prepared_schema={} lightweight={} full_reload={} pager_publication={}",
        report.connection_ceremony.prepared_schema_refreshes,
        report
            .connection_ceremony
            .prepared_schema_lightweight_refreshes,
        report.connection_ceremony.prepared_schema_full_reloads,
        report.connection_ceremony.pager_publication_refreshes
    );
    let _ = writeln!(
        out,
        "- Cached snapshot reuse/parks: {}/{}",
        report.connection_ceremony.cached_read_snapshot_reuses,
        report.connection_ceremony.cached_read_snapshot_parks
    );
    let _ = writeln!(
        out,
        "- Prepared engine fresh/reuse: {}/{}",
        report
            .connection_ceremony
            .prepared_table_engine_fresh_allocs,
        report.connection_ceremony.prepared_table_engine_reuses
    );
    let _ = writeln!(
        out,
        "- Prepared insert fast/instrumented lanes: {}/{}",
        report.connection_ceremony.prepared_insert_fast_lane_hits,
        report
            .connection_ceremony
            .prepared_insert_instrumented_lane_hits
    );
    let _ = writeln!(
        out,
        "- Prepared update/delete fast/instrumented lanes: {}/{}",
        report
            .connection_ceremony
            .prepared_update_delete_fast_lane_hits,
        report
            .connection_ceremony
            .prepared_update_delete_instrumented_lane_hits
    );
    let _ = writeln!(
        out,
        "- Prepared update/delete fallback reasons (returning/sqlite_sequence/without_rowid/live_vtab/trigger/fk): {}/{}/{}/{}/{}/{}",
        report
            .connection_ceremony
            .prepared_update_delete_fallback_returning,
        report
            .connection_ceremony
            .prepared_update_delete_fallback_sqlite_sequence,
        report
            .connection_ceremony
            .prepared_update_delete_fallback_without_rowid,
        report
            .connection_ceremony
            .prepared_update_delete_fallback_live_vtab,
        report
            .connection_ceremony
            .prepared_update_delete_fallback_trigger,
        report
            .connection_ceremony
            .prepared_update_delete_fallback_foreign_key
    );
    let _ = writeln!(
        out,
        "- Prepared DML affected-only runs: {}",
        report
            .connection_ceremony
            .prepared_table_dml_affected_only_runs
    );
    let _ = writeln!(
        out,
        "- sqlite_sequence fast-path/scan refresh: {}/{}",
        report
            .connection_ceremony
            .autoincrement_sequence_fast_path_updates,
        report
            .connection_ceremony
            .autoincrement_sequence_scan_refreshes
    );
    let _ = writeln!(
        out,
        "- Column-default evaluation passes: {}",
        report.connection_ceremony.column_default_evaluation_passes
    );
    let _ = writeln!(
        out,
        "- Statement lookaside alloc/reset/fallback: {}/{}/{}",
        report.connection_ceremony.arena_alloc_bytes,
        report.connection_ceremony.arena_reset_count,
        report
            .connection_ceremony
            .fallback_to_general_allocator_count
    );
    let _ = writeln!(
        out,
        "- Direct write flush calls/time_ns: {}/{}",
        report.connection_ceremony.direct_write_flush_calls,
        report.connection_ceremony.direct_write_flush_time_ns
    );
    let _ = writeln!(
        out,
        "- :memory: autocommit fast-path begins: {}",
        report
            .connection_ceremony
            .memory_autocommit_fast_path_begins
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "## MVCC Write Path\n");
    let _ = writeln!(
        out,
        "- Writes total: {} (tier0={}, tier1={}, tier2={})",
        report.mvcc_write.total_write_attempts,
        report.mvcc_write.tier0_already_owned_writes_total,
        report.mvcc_write.tier1_first_touch_writes_total,
        report.mvcc_write.tier2_commit_surface_writes_total
    );
    let _ = writeln!(
        out,
        "- Page-touch classes: already_owned={} first_touch={} commit_surface={} page_one_tracks={} pending_surface_clears={}",
        report.mvcc_write.tier0_already_owned_writes_total,
        report.mvcc_write.tier1_first_touch_writes_total,
        report.mvcc_write.tier2_commit_surface_writes_total,
        report.mvcc_write.page_one_conflict_tracks_total,
        report.mvcc_write.pending_commit_surface_clears_total
    );
    let _ = writeln!(
        out,
        "- Page-lock waits: {} (time_ns={})",
        report.mvcc_write.page_lock_waits_total, report.mvcc_write.page_lock_wait_time_ns_total
    );
    let _ = writeln!(
        out,
        "- BUSY retries/timeouts: {}/{}",
        report.mvcc_write.write_busy_retries_total, report.mvcc_write.write_busy_timeouts_total
    );
    let _ = writeln!(
        out,
        "- Runtime retry taxonomy: total={} aborts={} kind[busy={},busy_snapshot={},busy_recovery={},other={}] phase[begin={},body={},commit={},rollback={}] max_batch_attempts={}",
        report.mvcc_write.runtime_retry.total_retries,
        report.mvcc_write.runtime_retry.total_aborts,
        report.mvcc_write.runtime_retry.kind.busy,
        report.mvcc_write.runtime_retry.kind.busy_snapshot,
        report.mvcc_write.runtime_retry.kind.busy_recovery,
        report.mvcc_write.runtime_retry.kind.busy_other,
        report.mvcc_write.runtime_retry.phase.begin,
        report.mvcc_write.runtime_retry.phase.body,
        report.mvcc_write.runtime_retry.phase.commit,
        report.mvcc_write.runtime_retry.phase.rollback,
        report.mvcc_write.runtime_retry.max_batch_attempts
    );
    if !report
        .mvcc_write
        .runtime_retry
        .top_snapshot_conflict_pages
        .is_empty()
    {
        let top_pages = report
            .mvcc_write
            .runtime_retry
            .top_snapshot_conflict_pages
            .iter()
            .map(|entry| format!("p{}:{}", entry.page_no, entry.retries))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(out, "- Top snapshot conflict pages: {top_pages}");
    }
    if let Some(last_busy_message) = &report.mvcc_write.runtime_retry.last_busy_message {
        let _ = writeln!(out, "- Last retry message: `{last_busy_message}`");
    }
    let _ = writeln!(
        out,
        "- Stale snapshot rejects: {}",
        report.mvcc_write.stale_snapshot_rejects_total
    );
    let _ = writeln!(
        out,
        "- Page-one conflict tracking: {} (time_ns={})",
        report.mvcc_write.page_one_conflict_tracks_total,
        report.mvcc_write.page_one_conflict_track_time_ns_total
    );
    let _ = writeln!(
        out,
        "- Pending surface clears: {} (time_ns={})",
        report.mvcc_write.pending_commit_surface_clears_total,
        report.mvcc_write.pending_commit_surface_clear_time_ns_total
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "## Page Buffer Pool\n");
    let _ = writeln!(
        out,
        "- `page_buffer_pool_hit` / `page_buffer_pool_miss`: {}/{}",
        report.page_buffer_pool.page_buffer_pool_hits,
        report.page_buffer_pool.page_buffer_pool_misses
    );
    let _ = writeln!(
        out,
        "- `page_image_slab_reuse_count`: {}",
        report.page_buffer_pool.page_image_slab_reuse_count
    );
    let _ = writeln!(
        out,
        "- `staged_page_copy_bytes`: {}",
        report.page_buffer_pool.staged_page_copy_bytes
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "## PageData Motion\n");
    let _ = writeln!(
        out,
        "- Borrowed normalization calls: {} (exact-size copies={})",
        report
            .page_data_motion
            .borrowed_write_normalization_calls_total,
        report.page_data_motion.borrowed_exact_size_copies_total
    );
    let _ = writeln!(
        out,
        "- Owned normalization calls: {} (passthrough={}, in_place_zero_extends={}, resized_copies={})",
        report
            .page_data_motion
            .owned_write_normalization_calls_total,
        report.page_data_motion.owned_passthrough_total,
        report.page_data_motion.owned_in_place_zero_extends_total,
        report.page_data_motion.owned_resized_copies_total
    );
    let _ = writeln!(
        out,
        "- Normalized bytes: {} (payload={}, zero_fill={})",
        report.page_data_motion.normalized_bytes_total,
        report.page_data_motion.normalized_payload_bytes_total,
        report.page_data_motion.normalized_zero_fill_bytes_total
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "## B-Tree Copy Kernel Targets\n");
    if report.btree_copy_kernel_targets.is_empty() {
        let _ = writeln!(
            out,
            "- No copy-kernel bytes were recorded for this run; decode and cell assembly stayed on borrowed paths."
        );
    } else {
        for (rank, entry) in report.btree_copy_kernel_targets.iter().enumerate() {
            let _ = writeln!(
                out,
                "- rank {} {}: {}={} -> {}",
                rank + 1,
                entry.subsystem,
                entry.metric_kind,
                entry.metric_value,
                entry.rationale
            );
        }
        let _ = writeln!(
            out,
            "- note: this is a bytes-first replacement target list for later kernel work; it isolates owned-buffer and reassembly surfaces rather than claiming an exclusive wall-time partition."
        );
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

    let _ = writeln!(out, "## Baseline Reuse Ledger\n");
    for entry in &actionable_ranking.baseline_reuse_ledger {
        let hit_rate = entry
            .hit_rate_basis_points
            .map_or_else(|| "n/a".to_owned(), |value| value.to_string());
        let _ = writeln!(
            out,
            "- rank {} {}: supported={}, hits={}, misses={}, hit_rate_bps={} -> {} [{}]",
            entry.rank,
            entry.surface,
            entry.supported,
            entry.hits,
            entry.misses,
            hit_rate,
            entry.implication,
            entry.mapped_beads.join(", ")
        );
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Baseline Waste Ledger\n");
    for entry in &actionable_ranking.baseline_waste_ledger {
        let wall_share = entry
            .wall_share_basis_points
            .map_or_else(|| "n/a".to_owned(), |value| value.to_string());
        let _ = writeln!(
            out,
            "- rank {} {}: class={}, {}={}, wall_share_bps={}, allocator_pressure_bytes={}, activity_count={} -> {} [{}]",
            entry.rank,
            entry.component,
            entry.classification,
            entry.metric_kind,
            entry.metric_value,
            wall_share,
            entry.allocator_pressure_bytes,
            entry.activity_count,
            entry.implication,
            entry.mapped_beads.join(", ")
        );
    }
    let _ = writeln!(
        out,
        "- note: baseline and structural spillover entries are intentionally listed together here so low-retry rows can be separated from contention-driven wall time without hiding either class of cost."
    );
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

    let _ = writeln!(out, "## Wall-Time Decomposition\n");
    for entry in &actionable_ranking.wall_time_components {
        let _ = writeln!(
            out,
            "- rank {} {}: time_ns={}, wall_share_bps={} -> {} [{}]",
            entry.rank,
            entry.component,
            entry.time_ns,
            entry.wall_share_basis_points,
            entry.implication,
            entry.mapped_beads.join(", ")
        );
    }
    let _ = writeln!(
        out,
        "- note: component shares are evidence-backed but may overlap on multi-worker runs, so they should steer classification rather than be treated as an exclusive partition."
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "## Causal Classification\n");
    let _ = writeln!(
        out,
        "- Dominant bucket: {} (estimated_time_ns={}, wall_share_bps={}, score_bps={}, mixed_or_ambiguous={})",
        actionable_ranking.causal_classification.dominant_bucket,
        actionable_ranking
            .causal_classification
            .dominant_estimated_time_ns,
        actionable_ranking
            .causal_classification
            .dominant_wall_share_basis_points,
        actionable_ranking
            .causal_classification
            .dominant_score_basis_points,
        actionable_ranking.causal_classification.mixed_or_ambiguous
    );
    if let Some(runner_up_bucket) = &actionable_ranking.causal_classification.runner_up_bucket {
        let _ = writeln!(
            out,
            "- Runner-up: {} (estimated_time_ns={}, score_bps={}, gap_bps={})",
            runner_up_bucket,
            actionable_ranking
                .causal_classification
                .runner_up_estimated_time_ns
                .unwrap_or(0),
            actionable_ranking
                .causal_classification
                .runner_up_score_basis_points
                .unwrap_or(0),
            actionable_ranking
                .causal_classification
                .runner_up_gap_basis_points
                .unwrap_or(0)
        );
    }
    let _ = writeln!(
        out,
        "- Rationale: {}",
        actionable_ranking.causal_classification.rationale
    );
    for entry in &actionable_ranking.causal_buckets {
        let _ = writeln!(
            out,
            "- rank {} {}: dominant={}, estimated_time_ns={}, wall_share_bps={}, score_bps={} -> {} [{}]",
            entry.rank,
            entry.bucket,
            entry.dominant,
            entry.estimated_time_ns,
            entry.wall_share_basis_points,
            entry.score_basis_points,
            entry.implication,
            entry.mapped_beads.join(", ")
        );
        for evidence in &entry.evidence {
            let _ = writeln!(
                out,
                "  evidence {} {}: {}={} -> {}",
                evidence.artifact,
                evidence.metric_path,
                evidence.metric_kind,
                evidence.metric_value,
                evidence.rationale
            );
        }
    }
    let _ = writeln!(
        out,
        "- note: `mixed` becomes dominant when the leader concrete bucket is too small or within 1,000 score-bps of the runner-up, so near-ties stay explicit until more evidence lands."
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "## Microarchitectural Signatures\n");
    for entry in &actionable_ranking.microarchitectural_signatures {
        let secondary = if entry.secondary_signatures.is_empty() {
            String::from("none")
        } else {
            entry.secondary_signatures.join(", ")
        };
        let evidence = if entry.evidence_sources.is_empty() {
            String::from("none")
        } else {
            entry.evidence_sources.join(", ")
        };
        let _ = writeln!(
            out,
            "- rank {} {}: primary={}, secondary={}, confidence={} ({}bp), mixed={} -> {} [{}] evidence={}",
            entry.rank,
            entry.target,
            entry.primary_signature,
            secondary,
            entry.confidence_label,
            entry.confidence_score_basis_points,
            entry.mixed_or_ambiguous,
            entry.implication,
            entry.mapped_beads.join(", "),
            evidence
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
        "- `opcode_profile.json` — raw opcode totals for this profiled run"
    );
    let _ = writeln!(
        out,
        "- `subsystem_profile.json` — raw execution-subsystem timing, heap profile, WAL commit-path split/tail metrics, and B-tree copy-kernel target list for this run"
    );
    let _ = writeln!(
        out,
        "- `actionable_ranking.json` — hotspot, MVCC, reuse, and baseline-waste ledger for follow-on Track E/Track J work"
    );
    let _ = writeln!(
        out,
        "- `manifest.json` — replay metadata + artifact inventory"
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "## Mandatory Perf Checklist\n");
    let _ = writeln!(
        out,
        "- [ ] Confirm the cited claim matches this exact tuple: fixture `{}`, workload `{}`, concurrency `c{}`, concurrent mode `{}`.",
        report.fixture_id,
        report.workload,
        report.concurrency,
        if report.concurrent_mode { "ON" } else { "OFF" }
    );
    let _ = writeln!(
        out,
        "- [ ] Confirm the build/profile provenance is carried with the artifact bundle and that `manifest.json` plus the replay command remain attached to any benchmark note or PR."
    );
    let _ = writeln!(
        out,
        "- [ ] Confirm `summary.md`, `profile.json`, `subsystem_profile.json`, and `actionable_ranking.json` are published together before drawing conclusions from a single wall-time number."
    );
    let _ = writeln!(
        out,
        "- [ ] Confirm the top hotspot or causal bucket is translated into a concrete next action, including the mapped bead IDs or an explicit note that no mapped follow-up exists."
    );
    let _ = writeln!(
        out,
        "- [ ] Confirm any caveat from integrity-check state, retry behavior, or fallback counters is disclosed alongside the claim instead of being left implicit."
    );
    out
}

fn hotspot_implication(subsystem: &str) -> (&'static str, &'static [&'static str]) {
    match subsystem {
        "parser_ast_churn" => (
            "J2/J4 target: parser, AST, and compile churn should be reduced through prepared-artifact reuse and arena-backed scratch.",
            &["bd-db300.10.2", "bd-db300.10.4"],
        ),
        "record_decode" => (
            "J2/J5 target: row decode work is expensive enough to justify scratch-space reuse, decode caching, and copy reduction.",
            &["bd-db300.10.2", "bd-db300.10.5"],
        ),
        "row_materialization" => (
            "J2/J6/J7 target: result-row materialization is still paying avoidable clone/allocation and reusable-frame cost in the mixed hot path.",
            &["bd-db300.10.2", "bd-db300.10.6", "bd-db300.10.7"],
        ),
        "mvcc_page_lock_wait" => (
            "E2.1/E5.1 target: measured page-lock wait time is a first-class MVCC tax, so publish shrink and disjoint-page topology should move before deeper executor tuning.",
            &["bd-db300.5.2.1", "bd-db300.5.5.1"],
        ),
        "mvcc_page_one_conflict_tracking" => (
            "E5.1/E5.2 target: page-one conflict-only tracking is visible enough to justify home-lane/extent runway work that keeps disjoint inserts physically disjoint longer.",
            &["bd-db300.5.5.1", "bd-db300.5.5.2"],
        ),
        "mvcc_pending_commit_surface_clear" => (
            "E2.1/E2.2 target: synthetic pending-surface cleanup is now measured directly, so the next metadata-plane cut should shrink or bypass this publish-side maintenance.",
            &["bd-db300.5.2.1", "bd-db300.5.2.2"],
        ),
        _ => (
            "Secondary follow-up bucket after the named Track J hotspots.",
            &[],
        ),
    }
}

fn allocator_implication(subsystem: &str) -> (&'static str, &'static [&'static str]) {
    match subsystem {
        "result_row_values" => (
            "J2/J6/J7 target: emitted result rows are carrying most of the transient heap pressure and should benefit from ownership and frame reuse.",
            &["bd-db300.10.2", "bd-db300.10.6", "bd-db300.10.7"],
        ),
        "record_decode_values" => (
            "J2/J5 target: decoded record values create enough heap churn to justify scratch buffers and decode caching.",
            &["bd-db300.10.2", "bd-db300.10.5"],
        ),
        "parser_sql_bytes" => (
            "J2/J4 target: parse-volume churn is visible and should be reduced with reuse rather than repeated prepare work.",
            &["bd-db300.10.2", "bd-db300.10.4"],
        ),
        "page_data_normalization_bytes" => (
            "J3/J6 target: full-page normalization is materializing avoidable bytes before writes, so owned passthrough and reusable page buffers are still live optimization work.",
            &["bd-db300.10.3", "bd-db300.10.6"],
        ),
        _ => (
            "Secondary allocator-pressure source after the named Track J hotspots.",
            &[],
        ),
    }
}

fn cost_component_implication(component: &str) -> (&'static str, &'static [&'static str]) {
    match component {
        "parser_ast_churn" => (
            "J2/J4 target: parser and compile reuse still dominate this component enough to justify prepared-artifact work next.",
            &["bd-db300.10.2", "bd-db300.10.4"],
        ),
        "record_decode" => (
            "J2/J5 target: decode cost is large enough to justify scratch buffers and decode-cache work.",
            &["bd-db300.10.2", "bd-db300.10.5"],
        ),
        "row_materialization" => (
            "J2/J6/J7 target: emitted-row cloning and transient value ownership remain a first-class hot-path cost.",
            &["bd-db300.10.2", "bd-db300.10.6", "bd-db300.10.7"],
        ),
        "page_data_motion" => (
            "J3/J6 target: page-image normalization is now visible as its own copy lane, so reusable page buffers and owned passthrough should move before more speculative executor surgery.",
            &["bd-db300.10.3", "bd-db300.10.6"],
        ),
        _ => (
            "Secondary cost component after the named Track J follow-on buckets.",
            &[],
        ),
    }
}

fn wall_time_component_implication(component: &str) -> (&'static str, &'static [&'static str]) {
    match component {
        "queueing" => (
            "A/B target: retried BUSY attempts are consuming visible wall time before useful work resumes, so the handoff policy should be tightened before scaling further.",
            &["bd-db300.2.4"],
        ),
        "synchronization" => (
            "A/E target: transaction-boundary coordination is still a first-class wall-time tax and should be pushed toward narrower residual serialized regions.",
            &["bd-db300.1.5", "bd-db300.5.1"],
        ),
        "retry" => (
            "B target: sleep-based retry backoff is still showing up in the hot cell and should be replaced with a bounded handoff strategy.",
            &["bd-db300.2.4"],
        ),
        "service" => (
            "J target: useful body execution still dominates enough wall time that parser, decode, row-path, and residual VDBE/page-motion optimizations remain the main throughput lever once contention is under control.",
            &[
                "bd-db300.10.2",
                "bd-db300.10.4",
                "bd-db300.10.5",
                "bd-db300.10.6",
                "bd-db300.10.7",
                "bd-db300.10.8",
            ],
        ),
        "allocator_copy" => (
            "J target: allocator and copy work is large enough to justify scratch-space reuse, row-value ownership reduction, and reusable buffers.",
            &[
                "bd-db300.10.2",
                "bd-db300.10.3",
                "bd-db300.10.6",
                "bd-db300.10.7",
            ],
        ),
        "durability" => (
            "E target: commit durability is now an explicit measured lane, so future architecture work can separate durable ordering from general executor service cost.",
            &["bd-db300.5.1"],
        ),
        "mvcc_wait" => (
            "E2.1/E5.1 target: page-lock wait time is now explicit in the wall-time story, so tiny-publish and topological disjointness can be judged against a real synchronization lane instead of guesses.",
            &["bd-db300.5.2.1", "bd-db300.5.5.1"],
        ),
        "mvcc_commit_surface" => (
            "E2.1/E5.1 target: page-one tracking plus pending-surface maintenance are an explicit wall-time lane, which is the right steering signal for publish-plane shrink versus page-topology work.",
            &["bd-db300.5.2.1", "bd-db300.5.5.1"],
        ),
        _ => (
            "Secondary wall-time bucket after the named Track A steering categories.",
            &[],
        ),
    }
}

fn hot_path_signature_confidence_label(confidence_score_basis_points: u32) -> &'static str {
    match confidence_score_basis_points {
        8_000..=u32::MAX => "high",
        5_500..=7_999 => "medium",
        _ => "low",
    }
}

fn push_unique_signature_evidence(values: &mut Vec<String>, value: impl Into<String>) {
    let value = value.into();
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn top_subsystem_for_service(report: &HotPathProfileReport) -> &'static str {
    report
        .subsystem_ranking
        .iter()
        .find_map(|entry| match entry.subsystem.as_str() {
            "parser_ast_churn" => Some("parser_ast_churn"),
            "record_decode" => Some("record_decode"),
            "row_materialization" => Some("row_materialization"),
            _ => None,
        })
        .unwrap_or("service_mixed")
}

fn component_signature_shape(
    report: &HotPathProfileReport,
    component: &str,
    counter_capture_summary: Option<&HotPathCounterCaptureManifestSummary>,
) -> (
    &'static str,
    Vec<&'static str>,
    u32,
    bool,
    Vec<String>,
    String,
) {
    let mut evidence_sources = Vec::new();
    push_unique_signature_evidence(
        &mut evidence_sources,
        format!("wall_time_component:{component}"),
    );
    let has_capture = |capture: &str| {
        counter_capture_summary.is_some_and(|summary| {
            summary
                .host_capability_sensitive_captures
                .iter()
                .chain(summary.topology_sensitive_captures.iter())
                .any(|entry| entry == capture)
        })
    };
    let has_fallback_metric = |metric: &str| {
        counter_capture_summary.is_some_and(|summary| {
            summary
                .fallback_metric_pack
                .iter()
                .any(|entry| entry == metric)
        })
    };
    if has_capture("cache_to_cache") {
        push_unique_signature_evidence(
            &mut evidence_sources,
            "counter_capture:cache_to_cache(perf-c2c/perf-mem fallback)",
        );
    }
    if has_capture("remote_access") {
        push_unique_signature_evidence(
            &mut evidence_sources,
            "counter_capture:remote_access(perf-mem/perf-stat fallback)",
        );
    }
    if has_capture("migration") {
        push_unique_signature_evidence(
            &mut evidence_sources,
            "counter_capture:migration(cpu-migrations/context-switch fallback)",
        );
    }
    if has_capture("topdown") {
        push_unique_signature_evidence(
            &mut evidence_sources,
            "counter_capture:topdown(TopdownL1/core-event fallback)",
        );
    }
    if has_fallback_metric("cache-misses") {
        push_unique_signature_evidence(&mut evidence_sources, "fallback_metric:cache-misses");
    }

    match component {
        "durability" => {
            push_unique_signature_evidence(
                &mut evidence_sources,
                "wal_runtime:wal_service_us_total",
            );
            push_unique_signature_evidence(
                &mut evidence_sources,
                "wal_runtime:wal_append_us_total",
            );
            push_unique_signature_evidence(&mut evidence_sources, "wal_runtime:wal_sync_us_total");
            push_unique_signature_evidence(
                &mut evidence_sources,
                "wal_runtime:checkpoint_duration_us_total",
            );
            (
                "durability_pressure",
                Vec::new(),
                9_000,
                false,
                evidence_sources,
                String::from(
                    "WAL append, sync, and checkpoint service time are measured directly on the commit-finalize path, so this cell is a genuine durability-pressure lane rather than a generic executor slowdown.",
                ),
            )
        }
        "synchronization" => {
            let mut secondary = Vec::new();
            push_unique_signature_evidence(
                &mut evidence_sources,
                "wal_runtime:flusher_lock_wait_us_total",
            );
            push_unique_signature_evidence(
                &mut evidence_sources,
                "wal_runtime:wal_backend_lock_wait_us_total",
            );
            push_unique_signature_evidence(
                &mut evidence_sources,
                "wal_runtime:hist_wal_backend_lock_wait",
            );
            push_unique_signature_evidence(&mut evidence_sources, "wal_runtime:wake_reasons");
            if has_capture("remote_access") || has_capture("migration") {
                secondary.push("remote_numa_traffic");
            }
            (
                if has_capture("cache_to_cache") {
                    "ownership_ping_pong_hitm"
                } else {
                    "mixed_or_ambiguous"
                },
                secondary,
                if has_capture("cache_to_cache") {
                    7_000
                } else {
                    4_000
                },
                true,
                evidence_sources,
                String::from(
                    "BEGIN/COMMIT/ROLLBACK boundary time is visible, and the split WAL lock-wait plus wake evidence distinguishes coordination topology from durable append/sync service without inventing a single cause.",
                ),
            )
        }
        "mvcc_wait" => {
            push_unique_signature_evidence(
                &mut evidence_sources,
                "mvcc:page_lock_wait_time_ns_total",
            );
            let mut secondary = Vec::new();
            if has_capture("remote_access") || has_capture("migration") {
                secondary.push("remote_numa_traffic");
            }
            (
                if has_capture("cache_to_cache") {
                    "ownership_ping_pong_hitm"
                } else {
                    "mixed_or_ambiguous"
                },
                secondary,
                if has_capture("cache_to_cache") {
                    7_400
                } else if has_capture("remote_access") {
                    5_600
                } else {
                    4_200
                },
                true,
                evidence_sources,
                String::from(
                    "Measured page-lock wait time is the clearest MVCC coordination lane, and cache-to-cache or remote-access captures determine whether we can say more than an explicit mixed contention story.",
                ),
            )
        }
        "mvcc_commit_surface" => {
            push_unique_signature_evidence(
                &mut evidence_sources,
                "mvcc:page_one_conflict_track_time_ns_total",
            );
            push_unique_signature_evidence(
                &mut evidence_sources,
                "mvcc:pending_commit_surface_clear_time_ns_total",
            );
            let mut secondary = Vec::new();
            if has_capture("cache_to_cache") {
                secondary.push("ownership_ping_pong_hitm");
            }
            if has_capture("remote_access") || has_capture("migration") {
                secondary.push("remote_numa_traffic");
            }
            (
                "mixed_or_ambiguous",
                secondary,
                if has_capture("cache_to_cache") || has_capture("remote_access") {
                    5_800
                } else {
                    4_300
                },
                true,
                evidence_sources,
                String::from(
                    "Page-one conflict tracking plus synthetic pending-surface maintenance are measured directly, but the honest microarchitectural reading is still a mixed metadata/coordination lane unless stronger counter evidence is present.",
                ),
            )
        }
        "queueing" | "retry" => {
            let mut secondary = Vec::new();
            if has_capture("cache_to_cache") {
                secondary.push("ownership_ping_pong_hitm");
            }
            if has_capture("remote_access") || has_capture("migration") {
                secondary.push("remote_numa_traffic");
            }
            (
                "mixed_or_ambiguous",
                secondary,
                if has_capture("cache_to_cache") || has_capture("remote_access") {
                    5_500
                } else {
                    3_500
                },
                true,
                evidence_sources,
                String::from(
                    "BUSY/retry wall time proves contention is present, but the available evidence only narrows it to topology-sensitive coordination candidates rather than a single definitive microarchitectural culprit.",
                ),
            )
        }
        "allocator_copy" => (
            "llc_pressure",
            vec!["tlb_pressure"],
            if has_fallback_metric("cache-misses") {
                6_200
            } else {
                5_400
            },
            true,
            evidence_sources,
            String::from(
                "Copy-heavy row materialization is the clearest cache/memory lane in the hot path, and the capture pack includes cache-oriented evidence, so LLC/TLB pressure is the most defensible signature pair here.",
            ),
        ),
        "service" => match top_subsystem_for_service(report) {
            "parser_ast_churn" => (
                "front_end_starvation",
                vec!["branch_waste"],
                if has_capture("topdown") { 6_300 } else { 4_800 },
                true,
                {
                    push_unique_signature_evidence(
                        &mut evidence_sources,
                        "subsystem_hotspot:parser_ast_churn",
                    );
                    evidence_sources
                },
                String::from(
                    "Service time is dominated by parser/compile work, so front-end starvation with branch waste as a secondary signature is the most honest interpretation of the current evidence pack.",
                ),
            ),
            "record_decode" => (
                "llc_pressure",
                vec!["tlb_pressure"],
                if has_fallback_metric("cache-misses") || has_capture("remote_access") {
                    6_400
                } else {
                    5_000
                },
                true,
                {
                    push_unique_signature_evidence(
                        &mut evidence_sources,
                        "subsystem_hotspot:record_decode",
                    );
                    evidence_sources
                },
                String::from(
                    "Useful service work is concentrated in record decode, which is the strongest cache/memory consumer in the measured subsystem mix, so LLC/TLB pressure is the least speculative signature pair.",
                ),
            ),
            "row_materialization" => (
                "llc_pressure",
                vec!["remote_numa_traffic"],
                if has_capture("remote_access") || has_capture("cache_to_cache") {
                    5_900
                } else {
                    4_900
                },
                true,
                {
                    push_unique_signature_evidence(
                        &mut evidence_sources,
                        "subsystem_hotspot:row_materialization",
                    );
                    evidence_sources
                },
                String::from(
                    "Service time is being spent cloning row values, which is predominantly a cache/copy path; remote-access evidence keeps the cross-node possibility explicit instead of hiding it.",
                ),
            ),
            _ => (
                "mixed_or_ambiguous",
                Vec::new(),
                3_500,
                true,
                evidence_sources,
                String::from(
                    "Service time is present but the current profile does not justify a stronger microarchitectural story than an explicit mixed/ambiguous tag.",
                ),
            ),
        },
        _ => (
            "mixed_or_ambiguous",
            Vec::new(),
            3_000,
            true,
            evidence_sources,
            String::from(
                "This wall-time bucket is not yet strong enough to support a more specific microarchitectural classification.",
            ),
        ),
    }
}

fn build_hot_path_microarchitectural_signatures(
    report: &HotPathProfileReport,
    counter_capture_summary: Option<&HotPathCounterCaptureManifestSummary>,
    microarchitectural_context: Option<&HotPathMicroarchitecturalContext>,
) -> Vec<HotPathMicroarchitecturalSignatureEntry> {
    let fixture_id = microarchitectural_context.map_or_else(
        || report.fixture_id.clone(),
        |value| value.fixture_id.clone(),
    );
    let row_id = microarchitectural_context.map(|value| value.row_id.clone());
    let mode_id = microarchitectural_context.map(|value| value.mode_id.clone());
    let placement_profile_id =
        microarchitectural_context.and_then(|value| value.placement_profile_id.clone());
    let hardware_class_id =
        microarchitectural_context.and_then(|value| value.hardware_class_id.clone());
    let hardware_signature =
        microarchitectural_context.and_then(|value| value.hardware_signature.clone());
    let wall_time_components = build_hot_path_wall_time_components(report);
    let mut entries = Vec::with_capacity(wall_time_components.len());

    for component in wall_time_components {
        let (
            primary_signature,
            secondary_signatures,
            confidence_score_basis_points,
            mixed,
            evidence_sources,
            rationale,
        ) = component_signature_shape(report, &component.component, counter_capture_summary);
        let (implication, mapped_beads) = wall_time_component_implication(&component.component);
        entries.push(HotPathMicroarchitecturalSignatureEntry {
            rank: 0,
            target: component.component,
            primary_signature: primary_signature.to_owned(),
            secondary_signatures: secondary_signatures
                .into_iter()
                .map(str::to_owned)
                .collect(),
            confidence_label: hot_path_signature_confidence_label(confidence_score_basis_points)
                .to_owned(),
            confidence_score_basis_points,
            mixed_or_ambiguous: mixed,
            rationale,
            evidence_sources,
            fixture_id: fixture_id.clone(),
            row_id: row_id.clone(),
            mode_id: mode_id.clone(),
            placement_profile_id: placement_profile_id.clone(),
            hardware_class_id: hardware_class_id.clone(),
            hardware_signature: hardware_signature.clone(),
            implication: implication.to_owned(),
            mapped_beads: mapped_beads.iter().map(|bead| (*bead).to_owned()).collect(),
        });
    }

    entries.sort_by(|lhs, rhs| {
        rhs.confidence_score_basis_points
            .cmp(&lhs.confidence_score_basis_points)
            .then_with(|| lhs.target.cmp(&rhs.target))
    });
    for (rank, entry) in entries.iter_mut().enumerate() {
        entry.rank = u32::try_from(rank + 1).unwrap_or(u32::MAX);
    }
    entries
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

fn format_basis_points_ratio(value: u32) -> String {
    format!("{:.3}", f64::from(value) / 10_000.0)
}

fn hot_path_artifact_sha256(bytes: &[u8]) -> String {
    crate::bytes_to_lower_hex(Sha256::digest(bytes))
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

fn causal_evidence(
    artifact: &str,
    metric_path: &str,
    metric_kind: &str,
    metric_value: u64,
    rationale: &str,
) -> HotPathCausalEvidence {
    HotPathCausalEvidence {
        artifact: artifact.to_owned(),
        metric_path: metric_path.to_owned(),
        metric_kind: metric_kind.to_owned(),
        metric_value,
        rationale: rationale.to_owned(),
    }
}

fn find_wall_time_component<'a>(
    entries: &'a [HotPathWallTimeComponentEntry],
    component: &str,
) -> Option<&'a HotPathWallTimeComponentEntry> {
    entries.iter().find(|entry| entry.component == component)
}

fn find_cost_component<'a>(
    entries: &'a [HotPathCostComponentEntry],
    component: &str,
) -> Option<&'a HotPathCostComponentEntry> {
    entries.iter().find(|entry| entry.component == component)
}

fn find_baseline_waste_component<'a>(
    entries: &'a [HotPathBaselineWasteLedgerEntry],
    component: &str,
) -> Option<&'a HotPathBaselineWasteLedgerEntry> {
    entries.iter().find(|entry| entry.component == component)
}

fn push_mapped_beads(target: &mut Vec<String>, source: &[String]) {
    for bead in source {
        push_unique_signature_evidence(target, bead.clone());
    }
}

fn causal_bucket_implication(bucket: &str) -> &'static str {
    match bucket {
        "service" => {
            "Service-dominant rows should be explained with parser/decode/materialization evidence before blaming contention or durability."
        }
        "queueing" => {
            "Queueing-dominant rows are spending time inside failed BUSY attempts; steer fixes toward contention topology rather than allocator cleanup."
        }
        "synchronization" => {
            "Synchronization-dominant rows are paying boundary or MVCC coordination cost; keep publish-plane and page-topology work ahead of generic copy tuning."
        }
        "allocation" => {
            "Allocation-dominant rows are burning time or bytes on value/page copies; use the ownership-preserving and page-buffer lanes before broader executor work."
        }
        "io" => {
            "I/O-dominant rows are durability-bound, so WAL/VFS evidence should steer the next cut instead of retry or parser hypotheses."
        }
        "retries" => {
            "Retry-dominant rows are losing wall time to configured backoff, which is distinct from in-attempt queueing and should stay explicit."
        }
        "mixed" => {
            "Mixed stays available as a deliberate fallback when the top concrete buckets are too close to call with the current evidence pack."
        }
        _ => "Secondary causal bucket.",
    }
}

fn baseline_reuse_implication(surface: &str) -> (&'static str, &'static [&'static str]) {
    match surface {
        "statement_parse_cache" => (
            "J4 target: repeated parse misses still show avoidable prepare churn on the low-contention path.",
            &["bd-db300.10.4"],
        ),
        "compiled_plan_cache" => (
            "J4 target: compiled-plan misses are direct evidence for statement/plan caching work.",
            &["bd-db300.10.4"],
        ),
        "record_decode_cache" => (
            "J5 target: decode-cache hits/misses are now surfaced directly, so the next cuts should push the hit rate up and the invalidation counts down instead of treating decode churn as opaque.",
            &["bd-db300.10.5"],
        ),
        "cursor_frame_reuse" => (
            "J7 target: prepared engine fresh-vs-reuse counters now expose setup churn directly, so the next cuts should drive the reuse rate up instead of treating frame setup as opaque.",
            &["bd-db300.10.7"],
        ),
        "page_buffer_pool_reuse" => (
            "J3 target: page-buffer pool hits/misses and staged page-copy bytes are now explicit, so the next cuts should drive miss rate and copied bytes down instead of treating staging churn as opaque.",
            &["bd-db300.10.3"],
        ),
        "page_data_ownership_reuse" => (
            "J6 target: PageData ownership reuse is now measured directly, so next cuts can use passthrough, in-place zero-extend, and resized-copy evidence instead of treating ownership churn as a blind spot.",
            &["bd-db300.10.6"],
        ),
        _ => (
            "Secondary baseline reuse surface after the named Track J cache/reuse buckets.",
            &[],
        ),
    }
}

fn build_hot_path_baseline_reuse_ledger(
    report: &HotPathProfileReport,
) -> Vec<HotPathBaselineReuseLedgerEntry> {
    let parser_calls = report
        .parser
        .parse_single_calls
        .saturating_add(report.parser.parse_multi_calls);
    let decode_calls = report
        .record_decode
        .parse_record_into_calls
        .saturating_add(report.record_decode.parse_record_column_calls);
    let dominant_full_decode_callsite = hottest_full_record_decode_callsite(
        &report.record_decode.callsite_breakdown,
    )
    .map(|(callsite, total)| {
        format!(" dominant full-record decode callsite: {callsite} ({total} parse_record calls).")
    })
    .unwrap_or_default();
    let zero_hit_without_revisit = report.record_decode.decode_cache_hits_total == 0
        && report
            .record_decode
            .decode_cache_invalidations_position_total
            == 0
        && report.record_decode.decode_cache_invalidations_write_total == 0
        && report.record_decode.decode_cache_invalidations_pseudo_total == 0
        && report.record_decode.vdbe_column_reads_total
            == report.record_decode.decode_cache_misses_total;
    let mut entries = vec![
        HotPathBaselineReuseLedgerEntry {
            rank: 0,
            surface: "statement_parse_cache".to_owned(),
            supported: true,
            hits: report.parser.parse_cache_hits,
            misses: report.parser.parse_cache_misses,
            hit_rate_basis_points: Some(ratio_basis_points(
                report.parser.parse_cache_hits,
                report
                    .parser
                    .parse_cache_hits
                    .saturating_add(report.parser.parse_cache_misses),
            )),
            rationale: format!(
                "parse cache hits/misses are measured directly across {} parse calls",
                parser_calls
            ),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineReuseLedgerEntry {
            rank: 0,
            surface: "compiled_plan_cache".to_owned(),
            supported: true,
            hits: report.parser.compiled_cache_hits,
            misses: report.parser.compiled_cache_misses,
            hit_rate_basis_points: Some(ratio_basis_points(
                report.parser.compiled_cache_hits,
                report
                    .parser
                    .compiled_cache_hits
                    .saturating_add(report.parser.compiled_cache_misses),
            )),
            rationale:
                "compiled-plan cache hits/misses are captured directly at the prepare/compile boundary"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineReuseLedgerEntry {
            rank: 0,
            surface: "prepared_statement_cache".to_owned(),
            supported: true,
            hits: report.parser.prepared_cache_hits,
            misses: report.parser.prepared_cache_misses,
            hit_rate_basis_points: Some(ratio_basis_points(
                report.parser.prepared_cache_hits,
                report
                    .parser
                    .prepared_cache_hits
                    .saturating_add(report.parser.prepared_cache_misses),
            )),
            rationale:
                "prepared-statement template cache hits/misses are measured at the `Connection::prepare()` boundary"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineReuseLedgerEntry {
            rank: 0,
            surface: "record_decode_cache".to_owned(),
            supported: true,
            hits: report.record_decode.decode_cache_hits_total,
            misses: report.record_decode.decode_cache_misses_total,
            hit_rate_basis_points: Some(ratio_basis_points(
                report.record_decode.decode_cache_hits_total,
                report
                    .record_decode
                    .decode_cache_hits_total
                    .saturating_add(report.record_decode.decode_cache_misses_total),
            )),
            rationale: format!(
                "record decode cache hits/misses are measured directly in the VDBE ({hits} hits, {misses} misses; invalidations: position={position}, write={write}, pseudo={pseudo}) across {decode_calls} decode entrypoints.{dominant}{no_revisit}",
                hits = report.record_decode.decode_cache_hits_total,
                misses = report.record_decode.decode_cache_misses_total,
                position = report.record_decode.decode_cache_invalidations_position_total,
                write = report.record_decode.decode_cache_invalidations_write_total,
                pseudo = report.record_decode.decode_cache_invalidations_pseudo_total,
                dominant = dominant_full_decode_callsite,
                no_revisit = if zero_hit_without_revisit {
                    " The representative path did not revisit any cached row image, so zero hits do not imply a broken cache."
                } else {
                    ""
                },
            ),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineReuseLedgerEntry {
            rank: 0,
            surface: "cursor_frame_reuse".to_owned(),
            supported: true,
            hits: report.connection_ceremony.prepared_table_engine_reuses,
            misses: report.connection_ceremony.prepared_table_engine_fresh_allocs,
            hit_rate_basis_points: Some(ratio_basis_points(
                report.connection_ceremony.prepared_table_engine_reuses,
                report
                    .connection_ceremony
                    .prepared_table_engine_reuses
                    .saturating_add(
                        report
                            .connection_ceremony
                            .prepared_table_engine_fresh_allocs,
                    ),
            )),
            rationale: format!(
                "prepared table execution now reports fresh engine allocations ({}) versus reuse hits ({}) directly",
                report.connection_ceremony.prepared_table_engine_fresh_allocs,
                report.connection_ceremony.prepared_table_engine_reuses
            ),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineReuseLedgerEntry {
            rank: 0,
            surface: "page_buffer_pool_reuse".to_owned(),
            supported: true,
            hits: report.page_buffer_pool.page_buffer_pool_hits,
            misses: report.page_buffer_pool.page_buffer_pool_misses,
            hit_rate_basis_points: Some(ratio_basis_points(
                report.page_buffer_pool.page_buffer_pool_hits,
                report
                    .page_buffer_pool
                    .page_buffer_pool_hits
                    .saturating_add(report.page_buffer_pool.page_buffer_pool_misses),
            )),
            rationale: format!(
                "hot-path artifacts now emit explicit page-buffer pool hits/misses ({hits}/{misses}), plus derived staged copy bytes ({copies}) and slab reuse count ({reuses}) for reusable page-image staging",
                hits = report.page_buffer_pool.page_buffer_pool_hits,
                misses = report.page_buffer_pool.page_buffer_pool_misses,
                copies = report.page_buffer_pool.staged_page_copy_bytes,
                reuses = report.page_buffer_pool.page_image_slab_reuse_count,
            ),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineReuseLedgerEntry {
            rank: 0,
            surface: "page_data_ownership_reuse".to_owned(),
            supported: true,
            hits: report
                .page_data_motion
                .owned_passthrough_total
                .saturating_add(report.page_data_motion.owned_in_place_zero_extends_total),
            misses: report
                .page_data_motion
                .owned_write_normalization_calls_total
                .saturating_sub(
                    report
                        .page_data_motion
                        .owned_passthrough_total
                        .saturating_add(report.page_data_motion.owned_in_place_zero_extends_total),
                ),
            hit_rate_basis_points: Some(ratio_basis_points(
                report
                    .page_data_motion
                    .owned_passthrough_total
                    .saturating_add(report.page_data_motion.owned_in_place_zero_extends_total),
                report.page_data_motion.owned_write_normalization_calls_total,
            )),
            rationale: format!(
                "owned PageData writes now report passthrough, in-place zero-extend, and resized-copy outcomes ({} passthrough, {} in-place zero-extends, {} resized copies); borrowed writes remain a separate explicit copy lane",
                report.page_data_motion.owned_passthrough_total,
                report.page_data_motion.owned_in_place_zero_extends_total,
                report.page_data_motion.owned_resized_copies_total
            ),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
    ];
    entries.sort_by(|lhs, rhs| {
        let lhs_supported_rank = if lhs.supported { 1u8 } else { 0u8 };
        let rhs_supported_rank = if rhs.supported { 1u8 } else { 0u8 };
        lhs_supported_rank
            .cmp(&rhs_supported_rank)
            .then_with(|| {
                lhs.hit_rate_basis_points
                    .unwrap_or(0)
                    .cmp(&rhs.hit_rate_basis_points.unwrap_or(0))
            })
            .then_with(|| {
                rhs.hits
                    .saturating_add(rhs.misses)
                    .cmp(&lhs.hits.saturating_add(lhs.misses))
            })
            .then_with(|| lhs.surface.cmp(&rhs.surface))
    });
    for (rank, entry) in entries.iter_mut().enumerate() {
        entry.rank = u32::try_from(rank + 1).unwrap_or(u32::MAX);
        let (implication, mapped_beads) = baseline_reuse_implication(&entry.surface);
        entry.implication = implication.to_owned();
        entry.mapped_beads = mapped_beads.iter().map(|bead| (*bead).to_owned()).collect();
    }
    entries
}

fn baseline_waste_implication(component: &str) -> (&'static str, &'static [&'static str]) {
    match component {
        "parser_prepare_churn" => (
            "J2/J4 target: parse/rewrite/compile work is still visible enough that caching and arena-backed scratch should move first.",
            &["bd-db300.10.2", "bd-db300.10.4"],
        ),
        "record_decode" => (
            "J2/J5 target: decode time and decoded-value heap churn remain direct baseline-tax candidates.",
            &["bd-db300.10.2", "bd-db300.10.5"],
        ),
        "row_materialization" => (
            "J2/J6/J7 target: emitted-row cloning is still paying avoidable heap and ownership cost on the common path.",
            &["bd-db300.10.2", "bd-db300.10.6", "bd-db300.10.7"],
        ),
        "executor_body_residual" => (
            "J6/J7/J8 target: residual service time beyond decode/materialization is where VDBE setup, cursor motion, page fetch, and ownership churn likely still hide.",
            &["bd-db300.10.6", "bd-db300.10.7", "bd-db300.10.8"],
        ),
        "boundary_coordination" => (
            "Mixed lane: boundary coordination should stay visible so baseline fixes do not accidentally absorb residual commit/path coordination into the wrong bucket.",
            &["bd-db300.1.5", "bd-db300.5.1"],
        ),
        "busy_retry_queueing" => (
            "Structural spillover: retry and BUSY queueing are not baseline tax and should steer Track A work instead of J-lane fixes.",
            &["bd-db300.2.4"],
        ),
        "durability" => (
            "Mixed lane: durability must stay explicit so later baseline work does not overclaim gains that really belong to WAL/commit-path changes.",
            &["bd-db300.5.1"],
        ),
        "mvcc_page_lock_wait" => (
            "Structural spillover: measured page-lock wait is explicit MVCC contention tax and should steer tiny-publish and topology work before generic baseline cleanup.",
            &["bd-db300.5.2.1", "bd-db300.5.5.1"],
        ),
        "mvcc_commit_surface_maintenance" => (
            "Mixed lane: page-one tracking plus pending-surface maintenance are now measurable enough to steer publish-shrink versus topology work without hiding them inside generic service time.",
            &["bd-db300.5.2.1", "bd-db300.5.5.1"],
        ),
        "page_data_normalization" => (
            "J3/J6 target: page normalization bytes are now explicit baseline tax, so reusable page buffers and ownership-preserving writes can be prioritized with real evidence.",
            &["bd-db300.10.3", "bd-db300.10.6"],
        ),
        _ => (
            "Secondary baseline or spillover bucket after the named Track J categories.",
            &[],
        ),
    }
}

fn build_hot_path_baseline_waste_ledger(
    report: &HotPathProfileReport,
) -> Vec<HotPathBaselineWasteLedgerEntry> {
    let runtime_phase_timing = report
        .engine_report
        .runtime_phase_timing
        .unwrap_or_default();
    let wall_time_ns = report.engine_report.wall_time_ms.saturating_mul(1_000_000);
    let parser_time_ns = report
        .parser
        .parse_time_ns
        .saturating_add(report.parser.rewrite_time_ns)
        .saturating_add(report.parser.compile_time_ns);
    let decode_calls = report
        .record_decode
        .parse_record_into_calls
        .saturating_add(report.record_decode.parse_record_column_calls);
    let parser_calls = report
        .parser
        .parse_single_calls
        .saturating_add(report.parser.parse_multi_calls);
    let wal_durability_time_ns = wal_durability_time_ns(report);
    let durability_time_ns =
        wal_durability_time_ns.min(runtime_phase_timing.commit_finalize_time_ns);
    let boundary_coordination_time_ns = runtime_phase_timing
        .begin_boundary_time_ns
        .saturating_add(runtime_phase_timing.rollback_time_ns)
        .saturating_add(
            runtime_phase_timing
                .commit_finalize_time_ns
                .saturating_sub(durability_time_ns),
        );
    let executor_body_residual = runtime_phase_timing
        .body_execution_time_ns
        .saturating_sub(report.record_decode.decode_time_ns)
        .saturating_sub(
            report
                .row_materialization
                .result_row_materialization_time_ns_total,
        );
    let mvcc_commit_surface_time_ns = report
        .mvcc_write
        .page_one_conflict_track_time_ns_total
        .saturating_add(report.mvcc_write.pending_commit_surface_clear_time_ns_total);
    let busy_retry_queueing_time_ns = runtime_phase_timing
        .busy_attempt_time_ns
        .saturating_add(runtime_phase_timing.retry_backoff_time_ns);
    let mut entries = vec![
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "parser_prepare_churn".to_owned(),
            classification: "baseline_tax".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: parser_time_ns,
            wall_share_basis_points: Some(ratio_basis_points(parser_time_ns, wall_time_ns)),
            allocator_pressure_bytes: report.allocator_pressure.parser_sql_bytes,
            activity_count: parser_calls,
            rationale:
                "parse, rewrite, and compile time represent repeated prepare-path work on the common path"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "record_decode".to_owned(),
            classification: "baseline_tax".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: report.record_decode.decode_time_ns,
            wall_share_basis_points: Some(ratio_basis_points(
                report.record_decode.decode_time_ns,
                wall_time_ns,
            )),
            allocator_pressure_bytes: report.record_decode.vdbe_decoded_value_heap_bytes_total,
            activity_count: decode_calls,
            rationale:
                "record decode time and decoded-value heap bytes are measured directly in the VDBE hot path"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "row_materialization".to_owned(),
            classification: "baseline_tax".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: report
                .row_materialization
                .result_row_materialization_time_ns_total,
            wall_share_basis_points: Some(ratio_basis_points(
                report
                    .row_materialization
                    .result_row_materialization_time_ns_total,
                wall_time_ns,
            )),
            allocator_pressure_bytes: report.row_materialization.result_value_heap_bytes_total,
            activity_count: report.row_materialization.result_rows_total,
            rationale:
                "result-row materialization isolates emitted-value cloning and transient ownership work"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "executor_body_residual".to_owned(),
            classification: "mixed_or_residual".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: executor_body_residual,
            wall_share_basis_points: Some(ratio_basis_points(
                executor_body_residual,
                wall_time_ns,
            )),
            allocator_pressure_bytes: 0,
            activity_count: report.engine_report.ops_total,
            rationale:
                "body execution time that remains after explicit decode and row-materialization accounting; likely mixes VDBE dispatch, cursor motion, page fetch, and PageData ownership churn"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "boundary_coordination".to_owned(),
            classification: "mixed_or_residual".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: boundary_coordination_time_ns,
            wall_share_basis_points: Some(ratio_basis_points(
                boundary_coordination_time_ns,
                wall_time_ns,
            )),
            allocator_pressure_bytes: 0,
            activity_count: report.engine_report.ops_total,
            rationale:
                "BEGIN/ROLLBACK boundaries plus non-durable COMMIT coordination are real wall time, but they are not pure baseline tax and should stay separated"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "busy_retry_queueing".to_owned(),
            classification: "structural_side_effect".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: busy_retry_queueing_time_ns,
            wall_share_basis_points: Some(ratio_basis_points(
                busy_retry_queueing_time_ns,
                wall_time_ns,
            )),
            allocator_pressure_bytes: 0,
            activity_count: report.engine_report.retries,
            rationale:
                "BUSY-attempt time and retry-backoff sleep are contention spillover, not evidence of intrinsic single-writer baseline tax"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "durability".to_owned(),
            classification: "mixed_or_residual".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: durability_time_ns,
            wall_share_basis_points: Some(ratio_basis_points(durability_time_ns, wall_time_ns)),
            allocator_pressure_bytes: 0,
            activity_count: report.engine_report.ops_total,
            rationale:
                "WAL durability is an explicit measured lane and must remain visible so baseline optimization claims stay honest"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "mvcc_page_lock_wait".to_owned(),
            classification: "structural_side_effect".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: report.mvcc_write.page_lock_wait_time_ns_total,
            wall_share_basis_points: Some(ratio_basis_points(
                report.mvcc_write.page_lock_wait_time_ns_total,
                wall_time_ns,
            )),
            allocator_pressure_bytes: 0,
            activity_count: report.mvcc_write.page_lock_waits_total,
            rationale:
                "page-lock handoff wait time is measured directly inside the MVCC write helpers instead of being inferred from aggregate retries"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "mvcc_commit_surface_maintenance".to_owned(),
            classification: "mixed_or_residual".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: mvcc_commit_surface_time_ns,
            wall_share_basis_points: Some(ratio_basis_points(
                mvcc_commit_surface_time_ns,
                wall_time_ns,
            )),
            allocator_pressure_bytes: 0,
            activity_count: report
                .mvcc_write
                .page_one_conflict_tracks_total
                .saturating_add(report.mvcc_write.pending_commit_surface_clears_total),
            rationale:
                "page-one conflict tracking plus synthetic pending-surface cleanup are now explicit MVCC maintenance lanes"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathBaselineWasteLedgerEntry {
            rank: 0,
            component: "page_data_normalization".to_owned(),
            classification: "baseline_tax".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: report.page_data_motion.normalized_bytes_total,
            wall_share_basis_points: None,
            allocator_pressure_bytes: report.page_data_motion.normalized_bytes_total,
            activity_count: report
                .page_data_motion
                .borrowed_write_normalization_calls_total
                .saturating_add(report.page_data_motion.owned_write_normalization_calls_total),
            rationale:
                "page writes now expose payload-copy + zero-fill byte volume that still materializes while normalizing page images before pager entry"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
    ];
    entries.sort_by(|lhs, rhs| {
        rhs.metric_value
            .cmp(&lhs.metric_value)
            .then_with(|| lhs.component.cmp(&rhs.component))
    });
    for (rank, entry) in entries.iter_mut().enumerate() {
        entry.rank = u32::try_from(rank + 1).unwrap_or(u32::MAX);
        let (implication, mapped_beads) = baseline_waste_implication(&entry.component);
        entry.implication = implication.to_owned();
        entry.mapped_beads = mapped_beads.iter().map(|bead| (*bead).to_owned()).collect();
    }
    entries
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
        .saturating_add(report.allocator_pressure.result_value_heap_bytes_total)
        .saturating_add(
            report
                .allocator_pressure
                .page_data_normalization_bytes_total,
        );

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
        HotPathCostComponentEntry {
            rank: 0,
            component: "page_data_motion".to_owned(),
            time_ns: 0,
            time_share_basis_points: 0,
            allocator_pressure_bytes: report.allocator_pressure.page_data_normalization_bytes_total,
            allocator_share_basis_points: ratio_basis_points(
                report.allocator_pressure.page_data_normalization_bytes_total,
                total_allocator_pressure_bytes,
            ),
            activity_count: report
                .page_data_motion
                .borrowed_write_normalization_calls_total
                .saturating_add(report.page_data_motion.owned_write_normalization_calls_total),
            rationale:
                "write-path page normalization now surfaces payload-copy + zero-fill byte motion even when time attribution remains in the surrounding executor body"
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

fn build_hot_path_wall_time_components(
    report: &HotPathProfileReport,
) -> Vec<HotPathWallTimeComponentEntry> {
    let wall_time_ns = report.engine_report.wall_time_ms.saturating_mul(1_000_000);
    let allocator_copy_time_ns = report
        .row_materialization
        .result_row_materialization_time_ns_total;
    let runtime_phase_timing = report
        .engine_report
        .runtime_phase_timing
        .unwrap_or_default();
    let wal_durability_time_ns = wal_durability_time_ns(report);
    let durability_time_ns =
        wal_durability_time_ns.min(runtime_phase_timing.commit_finalize_time_ns);
    let synchronization_time_ns = runtime_phase_timing
        .begin_boundary_time_ns
        .saturating_add(runtime_phase_timing.rollback_time_ns)
        .saturating_add(
            runtime_phase_timing
                .commit_finalize_time_ns
                .saturating_sub(durability_time_ns),
        );
    let mvcc_wait_time_ns = report.mvcc_write.page_lock_wait_time_ns_total;
    let mvcc_commit_surface_time_ns = report
        .mvcc_write
        .page_one_conflict_track_time_ns_total
        .saturating_add(report.mvcc_write.pending_commit_surface_clear_time_ns_total);
    let service_time_ns = runtime_phase_timing
        .body_execution_time_ns
        .saturating_sub(allocator_copy_time_ns);

    let mut entries = vec![
        HotPathWallTimeComponentEntry {
            rank: 0,
            component: "queueing".to_owned(),
            time_ns: runtime_phase_timing.busy_attempt_time_ns,
            wall_share_basis_points: ratio_basis_points(
                runtime_phase_timing.busy_attempt_time_ns,
                wall_time_ns,
            ),
            rationale:
                "elapsed wall time spent inside batch attempts that ended in BUSY and had to queue for a later retry"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathWallTimeComponentEntry {
            rank: 0,
            component: "synchronization".to_owned(),
            time_ns: synchronization_time_ns,
            wall_share_basis_points: ratio_basis_points(synchronization_time_ns, wall_time_ns),
            rationale:
                "BEGIN/ROLLBACK boundary time plus non-durable COMMIT coordination observed directly by the executor"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathWallTimeComponentEntry {
            rank: 0,
            component: "retry".to_owned(),
            time_ns: runtime_phase_timing.retry_backoff_time_ns,
            wall_share_basis_points: ratio_basis_points(
                runtime_phase_timing.retry_backoff_time_ns,
                wall_time_ns,
            ),
            rationale:
                "configured backoff sleep requested after BUSY-family retries in the hot cell"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathWallTimeComponentEntry {
            rank: 0,
            component: "service".to_owned(),
            time_ns: service_time_ns,
            wall_share_basis_points: ratio_basis_points(service_time_ns, wall_time_ns),
            rationale:
                "batch body execution time outside the explicit allocator/copy bucket, capturing useful in-engine service work"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathWallTimeComponentEntry {
            rank: 0,
            component: "allocator_copy".to_owned(),
            time_ns: allocator_copy_time_ns,
            wall_share_basis_points: ratio_basis_points(allocator_copy_time_ns, wall_time_ns),
            rationale:
                "row materialization time measured inline at the VDBE boundary, isolating value cloning and copy-heavy result emission"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathWallTimeComponentEntry {
            rank: 0,
            component: "durability".to_owned(),
            time_ns: durability_time_ns,
            wall_share_basis_points: ratio_basis_points(durability_time_ns, wall_time_ns),
            rationale:
                "WAL append, sync, and checkpoint service time captured by runtime telemetry, clipped to observed COMMIT finalize time"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathWallTimeComponentEntry {
            rank: 0,
            component: "mvcc_wait".to_owned(),
            time_ns: mvcc_wait_time_ns,
            wall_share_basis_points: ratio_basis_points(mvcc_wait_time_ns, wall_time_ns),
            rationale:
                "page-lock handoff time measured inside the MVCC write helpers, separate from coarse retry/backoff wall time"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathWallTimeComponentEntry {
            rank: 0,
            component: "mvcc_commit_surface".to_owned(),
            time_ns: mvcc_commit_surface_time_ns,
            wall_share_basis_points: ratio_basis_points(
                mvcc_commit_surface_time_ns,
                wall_time_ns,
            ),
            rationale:
                "page-one conflict tracking plus synthetic pending-surface cleanup now form an explicit MVCC maintenance lane"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
    ];
    entries.sort_by(|lhs, rhs| {
        rhs.time_ns
            .cmp(&lhs.time_ns)
            .then_with(|| lhs.component.cmp(&rhs.component))
    });
    for (rank, entry) in entries.iter_mut().enumerate() {
        entry.rank = u32::try_from(rank + 1).unwrap_or(u32::MAX);
        let (implication, mapped_beads) = wall_time_component_implication(&entry.component);
        entry.implication = implication.to_owned();
        entry.mapped_beads = mapped_beads.iter().map(|bead| (*bead).to_owned()).collect();
    }
    entries
}

fn build_hot_path_causal_classification(
    report: &HotPathProfileReport,
    wall_time_components: &[HotPathWallTimeComponentEntry],
    cost_components: &[HotPathCostComponentEntry],
    baseline_waste_ledger: &[HotPathBaselineWasteLedgerEntry],
) -> (
    HotPathCausalClassificationSummary,
    Vec<HotPathCausalBucketEntry>,
) {
    let wall_time_ns = report.engine_report.wall_time_ms.saturating_mul(1_000_000);
    let runtime_phase_timing = report
        .engine_report
        .runtime_phase_timing
        .unwrap_or_default();
    let hot_path_profile = report.engine_report.hot_path_profile.as_ref();
    let queueing_time_ns =
        find_wall_time_component(wall_time_components, "queueing").map_or(0, |entry| entry.time_ns);
    let retry_time_ns =
        find_wall_time_component(wall_time_components, "retry").map_or(0, |entry| entry.time_ns);
    let service_time_ns =
        find_wall_time_component(wall_time_components, "service").map_or(0, |entry| entry.time_ns);
    let allocation_time_ns = find_wall_time_component(wall_time_components, "allocator_copy")
        .map_or(0, |entry| entry.time_ns);
    let io_time_ns = find_wall_time_component(wall_time_components, "durability")
        .map_or(0, |entry| entry.time_ns);
    let synchronization_boundary_time_ns =
        find_wall_time_component(wall_time_components, "synchronization")
            .map_or(0, |entry| entry.time_ns);
    let mvcc_wait_time_ns = find_wall_time_component(wall_time_components, "mvcc_wait")
        .map_or(0, |entry| entry.time_ns);
    let mvcc_commit_surface_time_ns =
        find_wall_time_component(wall_time_components, "mvcc_commit_surface")
            .map_or(0, |entry| entry.time_ns);
    let synchronization_time_ns = synchronization_boundary_time_ns
        .saturating_add(mvcc_wait_time_ns)
        .saturating_add(mvcc_commit_surface_time_ns);
    let classified_total_ns = service_time_ns
        .saturating_add(queueing_time_ns)
        .saturating_add(synchronization_time_ns)
        .saturating_add(allocation_time_ns)
        .saturating_add(io_time_ns)
        .saturating_add(retry_time_ns);
    let vfs_lock_ops = hot_path_profile.map_or(0, |profile| profile.vfs.lock_ops);
    let vfs_sync_ops = hot_path_profile.map_or(0, |profile| profile.vfs.sync_ops);
    let vfs_write_bytes_total = hot_path_profile.map_or(0, |profile| profile.vfs.write_bytes_total);
    let wal_frames_written_total =
        hot_path_profile.map_or(0, |profile| profile.wal.frames_written_total);
    let wal_bytes_written_total =
        hot_path_profile.map_or(0, |profile| profile.wal.bytes_written_total);
    let wal_commit_path_service_us_total =
        hot_path_profile.map_or(0, |profile| profile.wal.commit_path.wal_service_us_total);
    let wal_group_commit_latency_us_total =
        hot_path_profile.map_or(0, |profile| profile.wal.group_commit_latency_us_total);
    let runtime_retries_total = report.mvcc_write.runtime_retry.total_retries;
    let runtime_aborts_total = report.mvcc_write.runtime_retry.total_aborts;
    let page_data_normalization_bytes =
        find_baseline_waste_component(baseline_waste_ledger, "page_data_normalization")
            .map_or(0, |entry| entry.metric_value);
    let page_data_motion_allocator_bytes = find_cost_component(cost_components, "page_data_motion")
        .map_or(0, |entry| entry.allocator_pressure_bytes);
    let mut service_beads = Vec::new();
    if let Some(entry) = find_wall_time_component(wall_time_components, "service") {
        push_mapped_beads(&mut service_beads, &entry.mapped_beads);
    }
    for component in ["parser_ast_churn", "record_decode", "row_materialization"] {
        if let Some(entry) = find_cost_component(cost_components, component) {
            push_mapped_beads(&mut service_beads, &entry.mapped_beads);
        }
    }
    let mut queueing_beads = Vec::new();
    if let Some(entry) = find_wall_time_component(wall_time_components, "queueing") {
        push_mapped_beads(&mut queueing_beads, &entry.mapped_beads);
    }
    if let Some(entry) = find_baseline_waste_component(baseline_waste_ledger, "busy_retry_queueing")
    {
        push_mapped_beads(&mut queueing_beads, &entry.mapped_beads);
    }
    let mut synchronization_beads = Vec::new();
    for component in ["synchronization", "mvcc_wait", "mvcc_commit_surface"] {
        if let Some(entry) = find_wall_time_component(wall_time_components, component) {
            push_mapped_beads(&mut synchronization_beads, &entry.mapped_beads);
        }
    }
    let mut allocation_beads = Vec::new();
    if let Some(entry) = find_wall_time_component(wall_time_components, "allocator_copy") {
        push_mapped_beads(&mut allocation_beads, &entry.mapped_beads);
    }
    for component in ["row_materialization", "page_data_motion"] {
        if let Some(entry) = find_cost_component(cost_components, component) {
            push_mapped_beads(&mut allocation_beads, &entry.mapped_beads);
        }
    }
    if let Some(entry) =
        find_baseline_waste_component(baseline_waste_ledger, "page_data_normalization")
    {
        push_mapped_beads(&mut allocation_beads, &entry.mapped_beads);
    }
    let mut io_beads = Vec::new();
    if let Some(entry) = find_wall_time_component(wall_time_components, "durability") {
        push_mapped_beads(&mut io_beads, &entry.mapped_beads);
    }
    let mut retry_beads = Vec::new();
    if let Some(entry) = find_wall_time_component(wall_time_components, "retry") {
        push_mapped_beads(&mut retry_beads, &entry.mapped_beads);
    }
    if let Some(entry) = find_baseline_waste_component(baseline_waste_ledger, "busy_retry_queueing")
    {
        push_mapped_beads(&mut retry_beads, &entry.mapped_beads);
    }

    let service_evidence = vec![
        causal_evidence(
            "actionable_ranking.json",
            ".wall_time_components[] | select(.component == \"service\") | .time_ns",
            "time_ns",
            service_time_ns,
            "service starts from executor body time after the explicit allocator-copy carve-out",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.runtime_phase_timing.body_execution_time_ns",
            "time_ns",
            runtime_phase_timing.body_execution_time_ns,
            "raw executor body time anchors the useful-work lane before allocation is split out",
        ),
        causal_evidence(
            "actionable_ranking.json",
            ".cost_components[] | select(.component == \"parser_ast_churn\") | .time_ns",
            "time_ns",
            find_cost_component(cost_components, "parser_ast_churn")
                .map_or(0, |entry| entry.time_ns),
            "parser/prepare churn is already exposed as a named cost component inside service work",
        ),
        causal_evidence(
            "actionable_ranking.json",
            ".cost_components[] | select(.component == \"record_decode\") | .time_ns",
            "time_ns",
            find_cost_component(cost_components, "record_decode").map_or(0, |entry| entry.time_ns),
            "record decode remains one of the direct measured contributors to useful in-engine service time",
        ),
        causal_evidence(
            "actionable_ranking.json",
            ".cost_components[] | select(.component == \"row_materialization\") | .time_ns",
            "time_ns",
            find_cost_component(cost_components, "row_materialization")
                .map_or(0, |entry| entry.time_ns),
            "row materialization is kept visible so service and allocation can be reasoned about together instead of conflated",
        ),
    ];
    let queueing_evidence = vec![
        causal_evidence(
            "actionable_ranking.json",
            ".wall_time_components[] | select(.component == \"queueing\") | .time_ns",
            "time_ns",
            queueing_time_ns,
            "queueing is the wall-time lane for time spent inside BUSY attempts that later retried",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.runtime_phase_timing.busy_attempt_time_ns",
            "time_ns",
            runtime_phase_timing.busy_attempt_time_ns,
            "runtime phase timing captures the raw busy-attempt wall time directly",
        ),
        causal_evidence(
            "actionable_ranking.json",
            ".baseline_waste_ledger[] | select(.component == \"busy_retry_queueing\") | .metric_value",
            "time_ns",
            find_baseline_waste_component(baseline_waste_ledger, "busy_retry_queueing")
                .map_or(0, |entry| entry.metric_value),
            "the spillover ledger keeps busy-attempt queueing visible alongside the separate retry-backoff lane",
        ),
    ];
    let synchronization_evidence = vec![
        causal_evidence(
            "actionable_ranking.json",
            ".wall_time_components[] | select(.component == \"synchronization\") | .time_ns",
            "time_ns",
            synchronization_boundary_time_ns,
            "boundary coordination already has its own explicit wall-time component",
        ),
        causal_evidence(
            "actionable_ranking.json",
            ".wall_time_components[] | select(.component == \"mvcc_wait\") | .time_ns",
            "time_ns",
            mvcc_wait_time_ns,
            "page-lock handoff wait is the strongest direct MVCC coordination counter the profile exposes today",
        ),
        causal_evidence(
            "actionable_ranking.json",
            ".wall_time_components[] | select(.component == \"mvcc_commit_surface\") | .time_ns",
            "time_ns",
            mvcc_commit_surface_time_ns,
            "page-one tracking plus pending-surface maintenance are already split out as a separate coordination lane",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.hot_path_profile.vfs.lock_ops",
            "count",
            vfs_lock_ops,
            "VFS lock activity is emitted in the hot-path profile and helps explain synchronization-heavy rows without inventing a new counter",
        ),
    ];
    let allocation_evidence = vec![
        causal_evidence(
            "actionable_ranking.json",
            ".wall_time_components[] | select(.component == \"allocator_copy\") | .time_ns",
            "time_ns",
            allocation_time_ns,
            "allocator/copy wall time is already isolated at the VDBE boundary",
        ),
        causal_evidence(
            "actionable_ranking.json",
            ".baseline_waste_ledger[] | select(.component == \"page_data_normalization\") | .metric_value",
            "bytes",
            page_data_normalization_bytes,
            "page normalization bytes are the clearest existing artifact for write-path allocation and copy pressure",
        ),
        causal_evidence(
            "actionable_ranking.json",
            ".cost_components[] | select(.component == \"page_data_motion\") | .allocator_pressure_bytes",
            "bytes",
            page_data_motion_allocator_bytes,
            "page-data motion already exposes allocator pressure even when the surrounding wall time lives inside service work",
        ),
        causal_evidence(
            "profile.json",
            ".allocator_pressure.result_value_heap_bytes_total",
            "bytes",
            report.allocator_pressure.result_value_heap_bytes_total,
            "result-row heap bytes keep value materialization pressure attached to the allocation bucket",
        ),
    ];
    let io_evidence = vec![
        causal_evidence(
            "actionable_ranking.json",
            ".wall_time_components[] | select(.component == \"durability\") | .time_ns",
            "time_ns",
            io_time_ns,
            "durability is the explicit wall-time lane for commit-path I/O pressure",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.hot_path_profile.wal.commit_path.wal_service_us_total",
            "time_us",
            wal_commit_path_service_us_total,
            "commit-path WAL service time is the direct source for the durability wall-time lane when raw WAL telemetry is present",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.hot_path_profile.vfs.sync_ops",
            "count",
            vfs_sync_ops,
            "VFS sync operations are already exposed in the raw hot-path profile",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.hot_path_profile.vfs.write_bytes_total",
            "bytes",
            vfs_write_bytes_total,
            "VFS write-byte volume anchors file-system I/O intensity",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.hot_path_profile.wal.frames_written_total",
            "count",
            wal_frames_written_total,
            "WAL frames written quantify commit-path write volume without needing a new artifact",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.hot_path_profile.wal.bytes_written_total",
            "bytes",
            wal_bytes_written_total,
            "WAL byte volume is the clearest existing counter for log-heavy I/O rows",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.hot_path_profile.wal.group_commit_latency_us_total",
            "time_us",
            wal_group_commit_latency_us_total,
            "group-commit latency keeps the durability bucket connected to real commit-path timing",
        ),
    ];
    let retries_evidence = vec![
        causal_evidence(
            "actionable_ranking.json",
            ".wall_time_components[] | select(.component == \"retry\") | .time_ns",
            "time_ns",
            retry_time_ns,
            "retry bucket maps to explicit configured backoff sleep rather than in-attempt wait time",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.runtime_phase_timing.retry_backoff_time_ns",
            "time_ns",
            runtime_phase_timing.retry_backoff_time_ns,
            "runtime phase timing already exposes aggregate retry-backoff sleep directly",
        ),
        causal_evidence(
            "profile.json",
            ".engine_report.retries",
            "count",
            report.engine_report.retries,
            "top-level engine retries keep the wall-time lane tied to observed retry frequency",
        ),
        causal_evidence(
            "profile.json",
            ".mvcc_write.runtime_retry.total_retries",
            "count",
            runtime_retries_total,
            "structured retry taxonomy refines the retry bucket without collapsing it into generic queueing",
        ),
        causal_evidence(
            "profile.json",
            ".mvcc_write.runtime_retry.total_aborts",
            "count",
            runtime_aborts_total,
            "abort counts remain adjacent evidence for retry-heavy rows that fail to converge cleanly",
        ),
    ];

    let mut entries = vec![
        HotPathCausalBucketEntry {
            rank: 0,
            bucket: "service".to_owned(),
            dominant: false,
            estimated_time_ns: service_time_ns,
            wall_share_basis_points: ratio_basis_points(service_time_ns, wall_time_ns),
            score_basis_points: ratio_basis_points(service_time_ns, classified_total_ns),
            rationale: "Service bucket is anchored to executor body time after allocator-copy work is carved out, then cross-checked against parser/decode/materialization artifacts already emitted in the actionable ranking.".to_owned(),
            implication: causal_bucket_implication("service").to_owned(),
            mapped_beads: service_beads,
            evidence: service_evidence,
        },
        HotPathCausalBucketEntry {
            rank: 0,
            bucket: "queueing".to_owned(),
            dominant: false,
            estimated_time_ns: queueing_time_ns,
            wall_share_basis_points: ratio_basis_points(queueing_time_ns, wall_time_ns),
            score_basis_points: ratio_basis_points(queueing_time_ns, classified_total_ns),
            rationale: "Queueing bucket uses the explicit BUSY-attempt wall-time lane and keeps it separate from retry-backoff sleep.".to_owned(),
            implication: causal_bucket_implication("queueing").to_owned(),
            mapped_beads: queueing_beads,
            evidence: queueing_evidence,
        },
        HotPathCausalBucketEntry {
            rank: 0,
            bucket: "synchronization".to_owned(),
            dominant: false,
            estimated_time_ns: synchronization_time_ns,
            wall_share_basis_points: ratio_basis_points(synchronization_time_ns, wall_time_ns),
            score_basis_points: ratio_basis_points(synchronization_time_ns, classified_total_ns),
            rationale: "Synchronization bucket intentionally aggregates executor boundary coordination, MVCC page-lock wait, and commit-surface maintenance because those are the explicit coordination counters the code exposes today.".to_owned(),
            implication: causal_bucket_implication("synchronization").to_owned(),
            mapped_beads: synchronization_beads,
            evidence: synchronization_evidence,
        },
        HotPathCausalBucketEntry {
            rank: 0,
            bucket: "allocation".to_owned(),
            dominant: false,
            estimated_time_ns: allocation_time_ns,
            wall_share_basis_points: ratio_basis_points(allocation_time_ns, wall_time_ns),
            score_basis_points: ratio_basis_points(allocation_time_ns, classified_total_ns),
            rationale: "Allocation bucket starts with explicit allocator-copy wall time and is reinforced by emitted heap-byte and page-normalization artifacts.".to_owned(),
            implication: causal_bucket_implication("allocation").to_owned(),
            mapped_beads: allocation_beads,
            evidence: allocation_evidence,
        },
        HotPathCausalBucketEntry {
            rank: 0,
            bucket: "io".to_owned(),
            dominant: false,
            estimated_time_ns: io_time_ns,
            wall_share_basis_points: ratio_basis_points(io_time_ns, wall_time_ns),
            score_basis_points: ratio_basis_points(io_time_ns, classified_total_ns),
            rationale: "I/O bucket maps directly onto the durability wall-time lane and the VFS/WAL byte, frame, and sync counters already present in the raw profile artifact.".to_owned(),
            implication: causal_bucket_implication("io").to_owned(),
            mapped_beads: io_beads,
            evidence: io_evidence,
        },
        HotPathCausalBucketEntry {
            rank: 0,
            bucket: "retries".to_owned(),
            dominant: false,
            estimated_time_ns: retry_time_ns,
            wall_share_basis_points: ratio_basis_points(retry_time_ns, wall_time_ns),
            score_basis_points: ratio_basis_points(retry_time_ns, classified_total_ns),
            rationale: "Retries bucket isolates configured backoff sleep and retry counters so passive waiting is not conflated with queueing or synchronization work.".to_owned(),
            implication: causal_bucket_implication("retries").to_owned(),
            mapped_beads: retry_beads,
            evidence: retries_evidence,
        },
    ];
    let mut concrete_ranking = entries.clone();
    concrete_ranking.sort_by(|lhs, rhs| {
        rhs.score_basis_points
            .cmp(&lhs.score_basis_points)
            .then_with(|| rhs.estimated_time_ns.cmp(&lhs.estimated_time_ns))
            .then_with(|| lhs.bucket.cmp(&rhs.bucket))
    });
    let leader = concrete_ranking.first().cloned();
    let runner_up = concrete_ranking.get(1).cloned();
    let runner_up_gap_basis_points = match (&leader, &runner_up) {
        (Some(leader), Some(runner_up)) => Some(
            leader
                .score_basis_points
                .saturating_sub(runner_up.score_basis_points),
        ),
        _ => None,
    };
    let mixed_or_ambiguous = match (&leader, &runner_up) {
        (Some(leader), Some(runner_up)) => {
            leader.estimated_time_ns == 0
                || leader.score_basis_points <= 2_500
                || leader
                    .score_basis_points
                    .saturating_sub(runner_up.score_basis_points)
                    <= 1_000
        }
        (Some(leader), None) => leader.estimated_time_ns == 0 || leader.score_basis_points <= 2_500,
        (None, _) => true,
    };
    let mut mixed_beads = Vec::new();
    let mut mixed_evidence = Vec::new();
    if let Some(leader) = &leader {
        push_mapped_beads(&mut mixed_beads, &leader.mapped_beads);
        if let Some(evidence) = leader.evidence.first() {
            mixed_evidence.push(evidence.clone());
        }
    }
    if let Some(runner_up) = &runner_up {
        push_mapped_beads(&mut mixed_beads, &runner_up.mapped_beads);
        if let Some(evidence) = runner_up.evidence.first()
            && !mixed_evidence.iter().any(|existing| {
                existing.artifact == evidence.artifact
                    && existing.metric_path == evidence.metric_path
            })
        {
            mixed_evidence.push(evidence.clone());
        }
    }
    let mixed_time_ns = if mixed_or_ambiguous {
        leader
            .as_ref()
            .map_or(0, |entry| entry.estimated_time_ns)
            .saturating_add(
                runner_up
                    .as_ref()
                    .map_or(0, |entry| entry.estimated_time_ns),
            )
    } else {
        0
    };
    let mixed_wall_share_basis_points = ratio_basis_points(mixed_time_ns, wall_time_ns);
    let mixed_score_basis_points = ratio_basis_points(mixed_time_ns, classified_total_ns);
    let summary_rationale = match (&leader, &runner_up) {
        (Some(leader), Some(runner_up)) if mixed_or_ambiguous => format!(
            "Top concrete buckets `{}` ({}) and `{}` ({}) are still too close for a clean single-cause call, so the scaffold keeps a `mixed` lane until the upstream evidence beads land.",
            leader.bucket,
            leader.score_basis_points,
            runner_up.bucket,
            runner_up.score_basis_points
        ),
        (Some(leader), Some(runner_up)) => format!(
            "`{}` leads the concrete bucket ranking by {} score bps over `{}` using classified hot-path time derived from the existing wall-time, waste-ledger, and cost-component artifacts.",
            leader.bucket,
            leader
                .score_basis_points
                .saturating_sub(runner_up.score_basis_points),
            runner_up.bucket
        ),
        (Some(leader), None) if mixed_or_ambiguous => format!(
            "Only `{}` has a measurable classified signal and it remains too small to treat as a confident dominant cause, so the scaffold falls back to `mixed`.",
            leader.bucket
        ),
        (Some(leader), None) => format!(
            "`{}` is the only non-zero classified bucket exposed by the current artifact set.",
            leader.bucket
        ),
        (None, _) => "All classified causal buckets are currently zero, so the scaffold falls back to `mixed` until more evidence is captured.".to_owned(),
    };
    let dominant_bucket = if mixed_or_ambiguous {
        "mixed".to_owned()
    } else {
        leader
            .as_ref()
            .map_or_else(|| "mixed".to_owned(), |entry| entry.bucket.clone())
    };
    for entry in &mut entries {
        entry.dominant = !mixed_or_ambiguous
            && leader
                .as_ref()
                .is_some_and(|leader| entry.bucket == leader.bucket);
    }
    entries.push(HotPathCausalBucketEntry {
        rank: 0,
        bucket: "mixed".to_owned(),
        dominant: mixed_or_ambiguous,
        estimated_time_ns: mixed_time_ns,
        wall_share_basis_points: mixed_wall_share_basis_points,
        score_basis_points: mixed_score_basis_points,
        rationale: "Mixed is a deliberate fallback lane for rows where the concrete bucket leaderboard is too flat or too small to defend a single dominant cause with the current artifact set.".to_owned(),
        implication: causal_bucket_implication("mixed").to_owned(),
        mapped_beads: mixed_beads,
        evidence: mixed_evidence,
    });
    entries.sort_by(|lhs, rhs| {
        rhs.dominant
            .cmp(&lhs.dominant)
            .then_with(|| rhs.score_basis_points.cmp(&lhs.score_basis_points))
            .then_with(|| rhs.estimated_time_ns.cmp(&lhs.estimated_time_ns))
            .then_with(|| lhs.bucket.cmp(&rhs.bucket))
    });
    for (rank, entry) in entries.iter_mut().enumerate() {
        entry.rank = u32::try_from(rank + 1).unwrap_or(u32::MAX);
    }

    let summary = if mixed_or_ambiguous {
        HotPathCausalClassificationSummary {
            dominant_bucket,
            dominant_estimated_time_ns: mixed_time_ns,
            dominant_wall_share_basis_points: mixed_wall_share_basis_points,
            dominant_score_basis_points: mixed_score_basis_points,
            runner_up_bucket: leader.as_ref().map(|entry| entry.bucket.clone()),
            runner_up_estimated_time_ns: leader.as_ref().map(|entry| entry.estimated_time_ns),
            runner_up_score_basis_points: leader.as_ref().map(|entry| entry.score_basis_points),
            runner_up_gap_basis_points,
            mixed_or_ambiguous: true,
            rationale: summary_rationale,
        }
    } else if let Some(leader) = leader {
        HotPathCausalClassificationSummary {
            dominant_bucket,
            dominant_estimated_time_ns: leader.estimated_time_ns,
            dominant_wall_share_basis_points: leader.wall_share_basis_points,
            dominant_score_basis_points: leader.score_basis_points,
            runner_up_bucket: runner_up.as_ref().map(|entry| entry.bucket.clone()),
            runner_up_estimated_time_ns: runner_up.as_ref().map(|entry| entry.estimated_time_ns),
            runner_up_score_basis_points: runner_up.as_ref().map(|entry| entry.score_basis_points),
            runner_up_gap_basis_points,
            mixed_or_ambiguous: false,
            rationale: summary_rationale,
        }
    } else {
        HotPathCausalClassificationSummary {
            dominant_bucket,
            dominant_estimated_time_ns: 0,
            dominant_wall_share_basis_points: 0,
            dominant_score_basis_points: 0,
            runner_up_bucket: None,
            runner_up_estimated_time_ns: None,
            runner_up_score_basis_points: None,
            runner_up_gap_basis_points: None,
            mixed_or_ambiguous: true,
            rationale: summary_rationale,
        }
    };

    (summary, entries)
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
fn wal_hot_path_profile(report: &HotPathProfileReport) -> WalHotPathProfile {
    report
        .engine_report
        .hot_path_profile
        .as_ref()
        .map(|profile| profile.wal)
        .unwrap_or_default()
}

fn wal_durability_time_ns(report: &HotPathProfileReport) -> u64 {
    let wal = wal_hot_path_profile(report);
    wal.commit_path
        .wal_service_us_total
        .saturating_add(wal.checkpoint_duration_us_total)
        .saturating_mul(1_000)
}

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
        btree_copy_kernels: report.btree_copy_kernels.clone(),
        btree_copy_kernel_targets: report.btree_copy_kernel_targets.clone(),
        record_decode: report.record_decode.clone(),
        row_materialization: report.row_materialization.clone(),
        wal: wal_hot_path_profile(report),
        mvcc_write: report.mvcc_write.clone(),
        page_buffer_pool: report.page_buffer_pool.clone(),
        page_data_motion: report.page_data_motion.clone(),
        connection_ceremony: report.connection_ceremony.clone(),
    }
}

#[must_use]
pub fn build_hot_path_actionable_ranking(
    report: &HotPathProfileReport,
    counter_capture_summary: Option<&HotPathCounterCaptureManifestSummary>,
    microarchitectural_context: Option<&HotPathMicroarchitecturalContext>,
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
    let baseline_reuse_ledger = build_hot_path_baseline_reuse_ledger(report);
    let baseline_waste_ledger = build_hot_path_baseline_waste_ledger(report);
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
    let microarchitectural_signatures = build_hot_path_microarchitectural_signatures(
        report,
        counter_capture_summary,
        microarchitectural_context,
    );
    let wall_time_components = build_hot_path_wall_time_components(report);
    let cost_components = build_hot_path_cost_components(report);
    let (causal_classification, causal_buckets) = build_hot_path_causal_classification(
        report,
        &wall_time_components,
        &cost_components,
        &baseline_waste_ledger,
    );
    let top_opcodes = build_hot_path_opcode_profile(report)
        .opcodes
        .into_iter()
        .take(12)
        .collect();

    HotPathActionableRanking {
        schema_version: HOT_PATH_PROFILE_ACTIONABLE_RANKING_SCHEMA_V3.to_owned(),
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
        baseline_reuse_ledger,
        baseline_waste_ledger,
        named_hotspots,
        microarchitectural_signatures,
        wall_time_components,
        causal_classification,
        causal_buckets,
        cost_components,
        allocator_pressure,
        top_opcodes,
    }
}

pub fn write_hot_path_profile_artifacts(
    report: &HotPathProfileReport,
    output_dir: &Path,
    counter_capture_summary: Option<HotPathCounterCaptureManifestSummary>,
    provenance: Option<HotPathArtifactProvenance>,
    microarchitectural_context: Option<HotPathMicroarchitecturalContext>,
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
    let actionable_ranking = build_hot_path_actionable_ranking(
        report,
        counter_capture_summary.as_ref(),
        microarchitectural_context.as_ref(),
    );
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
        counter_capture_summary,
        provenance,
        files: vec![
            HotPathArtifactFile {
                path: "profile.json".to_owned(),
                bytes: u64::try_from(report_json.len()).unwrap_or(u64::MAX),
                sha256: hot_path_artifact_sha256(report_json.as_bytes()),
                description: "structured hot-path profile report".to_owned(),
            },
            HotPathArtifactFile {
                path: "opcode_profile.json".to_owned(),
                bytes: u64::try_from(opcode_profile_json.len()).unwrap_or(u64::MAX),
                sha256: hot_path_artifact_sha256(opcode_profile_json.as_bytes()),
                description: "raw opcode totals for the profiled run".to_owned(),
            },
            HotPathArtifactFile {
                path: "subsystem_profile.json".to_owned(),
                bytes: u64::try_from(subsystem_profile_json.len()).unwrap_or(u64::MAX),
                sha256: hot_path_artifact_sha256(subsystem_profile_json.as_bytes()),
                description:
                    "raw execution-subsystem timing, heap profile, and WAL commit-path split/tail metrics for the run"
                        .to_owned(),
            },
            HotPathArtifactFile {
                path: "summary.md".to_owned(),
                bytes: u64::try_from(summary_md.len()).unwrap_or(u64::MAX),
                sha256: hot_path_artifact_sha256(summary_md.as_bytes()),
                description: "human-readable hotspot ranking summary".to_owned(),
            },
            HotPathArtifactFile {
                path: "actionable_ranking.json".to_owned(),
                bytes: u64::try_from(actionable_ranking_json.len()).unwrap_or(u64::MAX),
                sha256: hot_path_artifact_sha256(actionable_ranking_json.as_bytes()),
                description:
                    "structured hotspot, reuse, and baseline-waste ledger for follow-on implementation work"
                        .to_owned(),
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
        sha256: hot_path_artifact_sha256(manifest_json.as_bytes()),
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

fn mode_id_for_engine(engine: Engine, concurrent_mode: bool) -> &'static str {
    match engine {
        Engine::Sqlite3 => "sqlite_reference",
        Engine::Fsqlite => {
            if concurrent_mode {
                "fsqlite_mvcc"
            } else {
                "fsqlite_single_writer"
            }
        }
    }
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
        let mode_id = mode_id_for_engine(cell.engine, config.settings.concurrent_mode).to_owned();
        return CellOutcome {
            summary: None,
            error: Some(format!("unknown workload preset: {}", cell.workload)),
            engine: cell.engine.as_str().to_owned(),
            mode_id: Some(mode_id),
            fixture_id: cell.fixture_id.clone(),
            workload: cell.workload.clone(),
            concurrency: cell.concurrency,
        };
    };

    let mode_id = mode_id_for_engine(cell.engine, config.settings.concurrent_mode).to_owned();
    let meta = BenchmarkMeta {
        engine: mode_id.clone(),
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

    let mut summary = run_benchmark(&config.benchmark_config, &meta, |iteration_idx| {
        run_single_iteration(
            engine,
            &fixture_id,
            &oplog,
            &workspace_config,
            &settings,
            iteration_idx,
        )
    });
    let comparison = BenchmarkComparisonMetadata::anonymous(&summary, mode_id.clone());
    summary.comparison = Some(comparison);
    let error = benchmark_summary_error(&summary);

    CellOutcome {
        summary: Some(summary),
        error,
        engine: cell.engine.as_str().to_owned(),
        mode_id: Some(mode_id),
        fixture_id: cell.fixture_id.clone(),
        workload: cell.workload.clone(),
        concurrency: cell.concurrency,
    }
}

fn benchmark_summary_error(summary: &BenchmarkSummary) -> Option<String> {
    let failed_iterations = summary
        .iterations
        .iter()
        .filter_map(|iteration| iteration.error.as_deref())
        .collect::<Vec<_>>();
    if failed_iterations.is_empty() {
        return None;
    }

    let failure_count = failed_iterations.len();
    let measurement_count = summary.iterations.len();
    let first_error = failed_iterations[0];
    if failure_count == measurement_count {
        Some(format!(
            "all {measurement_count} measurement iterations failed; first={first_error}"
        ))
    } else {
        Some(format!(
            "{failure_count}/{measurement_count} measurement iterations failed; first={first_error}"
        ))
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
        eprintln!("[perf] ({}/{}) running {}", i + 1, total, cell);

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
    use crate::report::StorageWiringReport;
    use fsqlite_types::record::{
        RecordHotPathProfileSnapshot, RecordProfileScopeBreakdownSnapshot, ValueTypeProfileSnapshot,
    };
    use fsqlite_vdbe::engine::{
        MvccWritePathMetricsSnapshot, OpcodeExecutionCount, PageDataMotionMetricsSnapshot,
        ValueTypeMetricsSnapshot, VdbeMetricsSnapshot,
    };

    fn hot_path_test_guard() -> std::sync::MutexGuard<'static, ()> {
        super::HOT_PATH_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn sample_engine_report() -> EngineRunReport {
        EngineRunReport {
            wall_time_ms: 42,
            ops_total: 17,
            ops_per_sec: 404.0,
            retries: 0,
            aborts: 0,
            correctness: crate::report::CorrectnessReport {
                raw_sha256_match: None,
                dump_match: None,
                canonical_sha256_match: None,
                integrity_check_ok: None,
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

    fn sample_hot_path_profile_report(
        config: &FsqliteHotPathProfileConfig,
    ) -> HotPathProfileReport {
        let storage_wiring = StorageWiringReport {
            backend_kind: "unix".to_owned(),
            backend_mode: "parity_cert_strict".to_owned(),
            backend_identity: "unix:parity_cert_strict".to_owned(),
            execution_lane_evidence: None,
        };
        let engine_report = EngineRunReport {
            wall_time_ms: 42,
            ops_total: 17,
            ops_per_sec: 404.0,
            retries: 0,
            aborts: 0,
            correctness: crate::report::CorrectnessReport {
                raw_sha256_match: None,
                dump_match: None,
                canonical_sha256_match: None,
                integrity_check_ok: None,
                raw_sha256: None,
                canonical_sha256: None,
                logical_sha256: None,
                notes: Some(
                    "mode=concurrent (MVCC); single-threaded sequential execution; backend_identity=unix:parity_cert_strict"
                        .to_owned(),
                ),
            },
            latency_ms: None,
            error: None,
            first_failure_diagnostic: None,
            storage_wiring: Some(storage_wiring),
            runtime_phase_timing: Some(crate::report::RuntimePhaseTimingEvidence {
                retry_backoff_time_ns: 800_000,
                busy_attempt_time_ns: 1_200_000,
                begin_boundary_time_ns: 500_000,
                body_execution_time_ns: 7_500,
                commit_finalize_time_ns: 700_000,
                rollback_time_ns: 300_000,
            }),
            hot_path_profile: None,
        };
        let snapshot = HotPathProfileSnapshot {
            parser: ParserHotPathProfileSnapshot {
                parse_single_calls: 3,
                parse_multi_calls: 1,
                parse_cache_hits: 2,
                parse_cache_misses: 2,
                parsed_sql_bytes: 256,
                parse_time_ns: 1_500,
                rewrite_calls: 2,
                rewrite_time_ns: 600,
                compiled_cache_hits: 1,
                compiled_cache_misses: 2,
                prepared_cache_hits: 1,
                prepared_cache_misses: 1,
                compile_time_ns: 900,
                fast_path_executions: 2,
                slow_path_executions: 1,
            },
            background_status_time_ns: 750,
            background_status_checks: 2,
            op_cx_background_gates: 1,
            statement_dispatch_background_gates: 1,
            prepared_lookup_time_ns: 1_100,
            prepared_schema_refresh_time_ns: 2_200,
            prepared_schema_refreshes: 1,
            prepared_schema_lightweight_refreshes: 1,
            prepared_schema_full_reloads: 0,
            pager_publication_refreshes: 1,
            memory_autocommit_fast_path_begins: 1,
            cached_read_snapshot_reuses: 1,
            cached_read_snapshot_parks: 1,
            begin_setup_time_ns: 3_300,
            begin_refresh_count: 1,
            commit_refresh_count: 1,
            memdb_refresh_count: 1,
            execute_body_time_ns: 4_400,
            direct_write_flush_calls: 2,
            direct_write_flush_time_ns: 4_800,
            commit_pre_txn_time_ns: 5_500,
            commit_txn_roundtrip_time_ns: 6_600,
            commit_finalize_seq_time_ns: 700,
            commit_handle_finalize_time_ns: 800,
            commit_post_write_maintenance_time_ns: 900,
            finalize_post_publish_time_ns: 1_250,
            concurrent_commit_plan_attempts: 0,
            concurrent_commit_plan_successes: 0,
            concurrent_commit_plan_errors: 0,
            concurrent_commit_plan_busy_snapshot_errors: 0,
            concurrent_commit_plan_pending_pages: 0,
            concurrent_commit_plan_write_pages: 0,
            concurrent_commit_plan_held_lock_pages: 0,
            concurrent_commit_plan_uncontended_fast_paths: 0,
            concurrent_commit_plan_candidate_free_fast_paths: 0,
            concurrent_commit_plan_full_validations: 0,
            pager_commit: fsqlite_pager::PagerCommitProfileSnapshot::default(),
            cached_write_txn_reuses: 1,
            cached_write_txn_parks: 1,
            retained_autocommit_reuses: 0,
            retained_autocommit_parks: 0,
            retained_autocommit_flushes: 0,
            retained_autocommit_read_after_write_flushes: 0,
            retained_autocommit_overlay_hits: 0,
            retained_autocommit_overlay_misses: 0,
            column_default_evaluation_passes: 2,
            prepared_table_engine_fresh_allocs: 1,
            prepared_table_engine_reuses: 1,
            prepared_insert_fast_lane_hits: 1,
            prepared_insert_instrumented_lane_hits: 0,
            prepared_update_delete_fast_lane_hits: 2,
            prepared_update_delete_instrumented_lane_hits: 1,
            prepared_update_delete_fallback_returning: 1,
            prepared_update_delete_fallback_sqlite_sequence: 0,
            prepared_update_delete_fallback_without_rowid: 0,
            prepared_update_delete_fallback_live_vtab: 0,
            prepared_update_delete_fallback_trigger: 1,
            prepared_update_delete_fallback_foreign_key: 1,
            prepared_table_dml_affected_only_runs: 1,
            prepared_update_delete_dml_direct_handoff_runs: 0,
            autoincrement_sequence_fast_path_updates: 1,
            autoincrement_sequence_scan_refreshes: 0,
            direct_indexed_equality_query_hits: 0,
            direct_rowid_range_query_hits: 0,
            direct_count_star_query_row_hits: 0,
            direct_rowid_lookup_query_row_hits: 0,
            direct_count_star_rowid_range_query_row_hits: 0,
            direct_count_indexed_rowid_probe_query_row_hits: 0,
            prepared_direct_insert_executions: 0,
            prepared_direct_update_executions: 0,
            prepared_direct_delete_executions: 0,
            prepared_direct_update_leaf_patch_run_start_attempts: 0,
            prepared_direct_update_leaf_patch_run_start_hits: 0,
            prepared_direct_update_leaf_patch_run_start_time_ns: 0,
            prepared_direct_update_leaf_patch_run_active_attempts: 0,
            prepared_direct_update_leaf_patch_run_active_hits: 0,
            prepared_direct_update_leaf_patch_run_active_misses: 0,
            prepared_direct_update_leaf_patch_run_active_time_ns: 0,
            prepared_direct_update_leaf_patch_run_flushes: 0,
            prepared_direct_update_leaf_patch_run_dirty_flushes: 0,
            prepared_direct_update_leaf_patch_run_flush_time_ns: 0,
            prepared_direct_delete_leaf_run_start_attempts: 0,
            prepared_direct_delete_leaf_run_start_hits: 0,
            prepared_direct_delete_leaf_run_start_time_ns: 0,
            prepared_direct_delete_leaf_run_active_attempts: 0,
            prepared_direct_delete_leaf_run_active_hits: 0,
            prepared_direct_delete_leaf_run_active_misses: 0,
            prepared_direct_delete_leaf_run_active_miss_shape_mismatches: 0,
            prepared_direct_delete_leaf_run_active_miss_staged_runs: 0,
            prepared_direct_delete_leaf_run_active_miss_rowid_not_in_leaf: 0,
            prepared_direct_delete_leaf_run_active_miss_already_deleted: 0,
            prepared_direct_delete_leaf_run_active_miss_nonroot_would_empty_leaf: 0,
            prepared_direct_delete_leaf_run_active_miss_nonroot_last_cell: 0,
            prepared_direct_delete_leaf_run_active_miss_noncompact_cell_area: 0,
            prepared_direct_delete_leaf_run_active_miss_cell_shape_or_overflow: 0,
            prepared_direct_delete_leaf_run_active_time_ns: 0,
            prepared_direct_delete_leaf_run_flushes: 0,
            prepared_direct_delete_leaf_run_dirty_flushes: 0,
            prepared_direct_delete_leaf_run_flush_time_ns: 0,
            prepared_direct_delete_preflush_time_ns: 0,
            prepared_direct_delete_rowid_lookup_time_ns: 0,
            prepared_direct_delete_active_leaf_probe_time_ns: 0,
            prepared_direct_delete_cursor_setup_time_ns: 0,
            prepared_direct_delete_memdb_abandon_calls: 0,
            prepared_direct_delete_memdb_abandon_time_ns: 0,
            prepared_direct_delete_memory_sync_calls: 0,
            prepared_direct_delete_memory_sync_time_ns: 0,
            prepared_direct_delete_qf_time_ns: 0,
            prepared_direct_delete_seek_time_ns: 0,
            prepared_direct_delete_physical_delete_time_ns: 0,
            prepared_direct_insert_row_build_time_ns: 0,
            prepared_direct_insert_preserialize_time_ns: 0,
            prepared_direct_insert_preserialize_cell_time_ns: 0,
            prepared_direct_insert_preserialize_eval_time_ns: 0,
            prepared_direct_insert_preserialize_affinity_time_ns: 0,
            prepared_direct_insert_preserialize_layout_time_ns: 0,
            prepared_direct_insert_preserialize_encode_time_ns: 0,
            prepared_direct_insert_row_value_build_time_ns: 0,
            prepared_direct_insert_cursor_setup_time_ns: 0,
            prepared_direct_insert_serialize_time_ns: 0,
            prepared_direct_insert_btree_insert_time_ns: 0,
            prepared_direct_insert_memdb_apply_time_ns: 0,
            prepared_direct_insert_schema_validation_time_ns: 0,
            prepared_direct_insert_autocommit_begin_time_ns: 0,
            prepared_direct_insert_change_tracking_time_ns: 0,
            prepared_direct_insert_autocommit_resolve_time_ns: 0,
            prepared_direct_insert_autocommit_executions: 0,
            prepared_direct_insert_page_run_flushes: 0,
            prepared_direct_insert_page_run_records: 0,
            prepared_direct_insert_page_run_bytes: 0,
            prepared_direct_insert_page_run_owned_flushes: 0,
            prepared_direct_insert_page_run_arena_flushes: 0,
            prepared_direct_insert_page_run_repeated_flushes: 0,
            prepared_direct_insert_page_run_empty_root_bulk_load_hits: 0,
            prepared_direct_insert_page_run_depth2_bulk_append_hits: 0,
            prepared_direct_insert_page_run_row_append_fallback_flushes: 0,
            prepared_direct_insert_page_run_row_append_fallback_rows: 0,
            arena_alloc_bytes: 2_048,
            arena_reset_count: 3,
            fallback_to_general_allocator_count: 1,
            page_buffer_pool_hits: 3,
            page_buffer_pool_misses: 1,
            window_func_partitions_total: 0,
            statement_global_executions: 4,
            statement_reuse_count: 2,
            statement_reuse_distance_sum: 3,
            statement_max_reuse_distance: 2,
            statement_lane_local_reuses: 2,
            statement_cross_lane_reuses: 0,
            statement_first_hit_count: 1,
            statement_first_hit_time_ns: 1_200,
            statement_warmed_hit_count: 3,
            statement_warmed_hit_time_ns: 2_100,
            background_compile_interference_count: 0,
            background_compile_interference_time_ns: 0,
            btree_copy_kernels: BtreeCopyProfileSnapshot {
                local_payload_copy_calls: 2,
                local_payload_copy_bytes: 96,
                owned_payload_materialization_calls: 1,
                owned_payload_materialization_bytes: 48,
                overflow_chain_reassembly_calls: 1,
                overflow_chain_local_bytes: 40,
                overflow_chain_overflow_bytes: 512,
                overflow_page_reads: 2,
                table_leaf_cell_assembly_calls: 1,
                table_leaf_cell_assembly_bytes: 56,
                index_leaf_cell_assembly_calls: 1,
                index_leaf_cell_assembly_bytes: 32,
                interior_cell_rebuild_calls: 1,
                interior_cell_rebuild_bytes: 44,
            },
            btree_leaf_reuse: BtreeLeafReuseSnapshot::default(),
            record_decode: RecordHotPathProfileSnapshot {
                parse_record_calls: 4,
                parse_record_into_calls: 2,
                parse_record_column_calls: 6,
                record_bytes_scanned: 512,
                record_vec_capacity_slots: 12,
                decode_time_ns: 3_000,
                decoded_values: ValueTypeProfileSnapshot {
                    null_count: 1,
                    integer_count: 5,
                    float_count: 1,
                    text_count: 2,
                    blob_count: 1,
                    text_bytes: 64,
                    blob_bytes: 32,
                },
                callsite_breakdown: RecordProfileScopeBreakdownSnapshot::default(),
            },
            vdbe: VdbeMetricsSnapshot {
                opcodes_executed_total: 120,
                statements_total: 8,
                statement_duration_us_total: 3_400,
                sort_rows_total: 0,
                sort_spill_pages_total: 0,
                opcode_execution_totals: vec![
                    OpcodeExecutionCount {
                        opcode: "Column".to_owned(),
                        total: 10,
                    },
                    OpcodeExecutionCount {
                        opcode: "ResultRow".to_owned(),
                        total: 8,
                    },
                    OpcodeExecutionCount {
                        opcode: "MakeRecord".to_owned(),
                        total: 4,
                    },
                ],
                type_coercions_total: 0,
                type_coercion_changes_total: 0,
                column_reads_total: 6,
                record_decode_calls_total: 4,
                decode_cache_hits_total: 4,
                decode_cache_misses_total: 3,
                decode_cache_invalidations_position_total: 2,
                decode_cache_invalidations_write_total: 1,
                decode_cache_invalidations_pseudo_total: 1,
                decoded_values_total: 10,
                decoded_value_heap_bytes_total: 96,
                result_rows_total: 4,
                result_values_total: 8,
                result_value_heap_bytes_total: 120,
                result_row_materialization_time_ns_total: 2_500,
                make_record_calls_total: 3,
                make_record_blob_bytes_total: 48,
                insert_append_count: 0,
                insert_seek_count: 0,
                insert_append_hint_clear_count: 0,
                decoded_value_types: ValueTypeMetricsSnapshot {
                    total_values: 10,
                    nulls: 1,
                    integers: 5,
                    reals: 1,
                    texts: 2,
                    blobs: 1,
                    text_bytes_total: 64,
                    blob_bytes_total: 32,
                },
                result_value_types: ValueTypeMetricsSnapshot {
                    total_values: 8,
                    nulls: 0,
                    integers: 4,
                    reals: 1,
                    texts: 2,
                    blobs: 1,
                    text_bytes_total: 48,
                    blob_bytes_total: 24,
                },
                mvcc_write_path: MvccWritePathMetricsSnapshot {
                    tier0_already_owned_writes_total: 1,
                    tier1_first_touch_writes_total: 3,
                    tier2_commit_surface_writes_total: 2,
                    page_lock_waits_total: 4,
                    page_lock_wait_time_ns_total: 2_400_000,
                    write_busy_retries_total: 3,
                    write_busy_timeouts_total: 1,
                    stale_snapshot_rejects_total: 2,
                    page_one_conflict_tracks_total: 2,
                    page_one_conflict_track_time_ns_total: 650_000,
                    pending_commit_surface_clears_total: 1,
                    pending_commit_surface_clear_time_ns_total: 250_000,
                },
                page_data_motion: PageDataMotionMetricsSnapshot {
                    borrowed_write_normalization_calls_total: 3,
                    borrowed_exact_size_copies_total: 2,
                    owned_write_normalization_calls_total: 5,
                    owned_passthrough_total: 3,
                    owned_in_place_zero_extends_total: 1,
                    owned_resized_copies_total: 2,
                    normalized_payload_bytes_total: 1_120,
                    normalized_zero_fill_bytes_total: 384,
                },
            },
        };

        build_hot_path_profile_report(
            "smoke",
            config,
            format!("{}-smoke", config.bead_id()),
            format!(
                "{}:smoke:c1",
                hot_path_profile_scenario_id(config.scenario_prefix(), &config.workload)
            ),
            engine_report,
            snapshot,
        )
    }

    #[test]
    fn benchmark_summary_error_reports_partial_measurement_failure() {
        let config = BenchmarkConfig {
            warmup_iterations: 0,
            min_iterations: 3,
            measurement_time_secs: 0,
        };
        let meta = BenchmarkMeta {
            engine: "sqlite3".to_owned(),
            workload: "inserts".to_owned(),
            fixture_id: "db-a".to_owned(),
            concurrency: 1,
            cargo_profile: "test".to_owned(),
        };
        let mut call_count = 0u32;
        let summary = run_benchmark(&config, &meta, |_| {
            call_count = call_count.saturating_add(1);
            if call_count == 2 {
                Err("setup failed")
            } else {
                Ok::<_, &str>(sample_engine_report())
            }
        });

        assert_eq!(
            benchmark_summary_error(&summary).as_deref(),
            Some("1/3 measurement iterations failed; first=setup failed")
        );
    }

    #[test]
    fn benchmark_summary_error_reports_total_measurement_failure() {
        let config = BenchmarkConfig {
            warmup_iterations: 0,
            min_iterations: 2,
            measurement_time_secs: 0,
        };
        let meta = BenchmarkMeta {
            engine: "sqlite3".to_owned(),
            workload: "inserts".to_owned(),
            fixture_id: "db-a".to_owned(),
            concurrency: 1,
            cargo_profile: "test".to_owned(),
        };
        let summary = run_benchmark(&config, &meta, |_| Err::<EngineRunReport, _>("open failed"));

        assert_eq!(
            benchmark_summary_error(&summary).as_deref(),
            Some("all 2 measurement iterations failed; first=open failed")
        );
    }

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

    fn focused_perf_workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("workspace root")
            .to_path_buf()
    }

    fn run_focused_disjoint_c8_case(
        mode_id: &str,
        engine: Engine,
        concurrent_mode: bool,
    ) -> BenchmarkSummary {
        let oplog = generate_oplog(
            "commutative_inserts_disjoint_keys",
            "frankensqlite",
            42,
            8,
            50,
        )
        .expect("known preset");
        let workspace_root = focused_perf_workspace_root();
        let workspace_config = WorkspaceConfig {
            golden_dir: workspace_root.join("sample_sqlite_db_files/golden"),
            working_base: workspace_root.join("sample_sqlite_db_files/working"),
        };
        let settings = HarnessSettings {
            concurrent_mode,
            ..HarnessSettings::default()
        };
        let benchmark_config = BenchmarkConfig {
            warmup_iterations: 0,
            min_iterations: 1,
            measurement_time_secs: 0,
        };
        let meta = BenchmarkMeta {
            engine: mode_id.to_owned(),
            workload: "commutative_inserts_disjoint_keys".to_owned(),
            fixture_id: "frankensqlite".to_owned(),
            concurrency: 8,
            cargo_profile: "release-perf".to_owned(),
        };
        let summary = run_benchmark(&benchmark_config, &meta, |iteration_idx| {
            run_single_iteration(
                engine,
                "frankensqlite",
                &oplog,
                &workspace_config,
                &settings,
                iteration_idx,
            )
        });
        let error = benchmark_summary_error(&summary);
        assert!(
            error.is_none(),
            "focused disjoint c8 run failed for {mode_id}: {error:?}"
        );
        summary
    }

    #[test]
    #[ignore = "manual remote perf evidence for the flagship c8 row"]
    fn focused_disjoint_c8_release_perf_triplet_emits_results() {
        let cases = [
            ("sqlite3", Engine::Sqlite3, true),
            ("fsqlite_mvcc", Engine::Fsqlite, true),
            ("fsqlite_single_writer", Engine::Fsqlite, false),
        ];

        let summaries = cases
            .into_iter()
            .map(|(mode_id, engine, concurrent_mode)| {
                eprintln!("[focused_disjoint_c8] starting {mode_id}");
                let summary = run_focused_disjoint_c8_case(mode_id, engine, concurrent_mode);
                eprintln!(
                    "[focused_disjoint_c8] finished {mode_id} median_ops_per_sec={:.2} mean_ops_per_sec={:.2}",
                    summary.throughput.median_ops_per_sec,
                    summary.throughput.mean_ops_per_sec
                );
                (
                    mode_id,
                    summary,
                )
            })
            .collect::<Vec<_>>();

        let artifact_dir =
            focused_perf_workspace_root().join("artifacts/perf/20260317_targeted_disjoint_c8");
        std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");

        let payload = serde_json::json!({
            "schema_version": "focused_disjoint_c8_release_perf_test.v1",
            "fixture_id": "frankensqlite",
            "workload": "commutative_inserts_disjoint_keys",
            "concurrency": 8,
            "seed": 42,
            "scale": 50,
            "cases": summaries.iter().map(|(mode_id, summary)| {
                serde_json::json!({
                    "mode_id": mode_id,
                    "benchmark_id": summary.benchmark_id,
                    "mean_ops_per_sec": summary.throughput.mean_ops_per_sec,
                    "median_ops_per_sec": summary.throughput.median_ops_per_sec,
                    "peak_ops_per_sec": summary.throughput.peak_ops_per_sec,
                    "iterations": summary.iterations,
                })
            }).collect::<Vec<_>>(),
        });
        std::fs::write(
            artifact_dir.join("focused_disjoint_c8_release_perf_test.json"),
            serde_json::to_string_pretty(&payload).expect("serialize focused result"),
        )
        .expect("write focused artifact");
        eprintln!(
            "FOCUSED_DISJOINT_C8_RELEASE_PERF_TEST_JSON:{}",
            serde_json::to_string(&payload).expect("serialize focused stdout payload")
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
            mode_id: Some("sqlite_reference".to_owned()),
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
                mode_id: Some("fsqlite_mvcc".to_owned()),
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
        let config = FsqliteHotPathProfileConfig {
            workload: "hot_page_contention".to_owned(),
            seed: 7,
            scale: 6,
            concurrency: 1,
            exec_config: crate::fsqlite_executor::FsqliteExecConfig {
                concurrent_mode: true,
                run_integrity_check: false,
                ..crate::fsqlite_executor::FsqliteExecConfig::default()
            },
            replay_command:
                "cargo run -p fsqlite-e2e --bin realdb-e2e -- hot-profile --db smoke --workload hot_page_contention"
                    .to_owned(),
            golden_dir: Some("sample_sqlite_db_files/golden".to_owned()),
            working_base: Some("sample_sqlite_db_files/working".to_owned()),
            bead_id: None,
            scenario_prefix: None,
        };

        let report = sample_hot_path_profile_report(&config);

        assert_eq!(report.schema_version, HOT_PATH_PROFILE_SCHEMA_V1);
        assert_eq!(report.fixture_id, "smoke");
        assert_eq!(report.workload, "hot_page_contention");
        assert_eq!(report.scenario_id, "bd-db300.4.1.hot_page_contention");
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
        assert!(!report.btree_copy_kernel_targets.is_empty());
        assert_eq!(
            report.btree_copy_kernel_targets[0].subsystem,
            "btree_overflow_reassembly"
        );
        assert!(report.mvcc_write.page_lock_wait_time_ns_total > 0);
        assert_eq!(report.page_buffer_pool.page_buffer_pool_hits, 3);
        assert_eq!(report.page_buffer_pool.page_buffer_pool_misses, 1);
        assert_eq!(report.page_buffer_pool.page_image_slab_reuse_count, 3);
        assert_eq!(report.page_buffer_pool.staged_page_copy_bytes, 1_504);
        assert!(report.page_data_motion.normalized_bytes_total > 0);
        assert_eq!(report.connection_ceremony.background_status_time_ns, 750);
        assert_eq!(report.connection_ceremony.prepared_lookup_time_ns, 1_100);
        assert_eq!(report.connection_ceremony.begin_setup_time_ns, 3_300);
        assert_eq!(report.connection_ceremony.execute_body_time_ns, 4_400);
        assert_eq!(report.connection_ceremony.direct_write_flush_calls, 2);
        assert_eq!(report.connection_ceremony.direct_write_flush_time_ns, 4_800);
        assert_eq!(report.connection_ceremony.commit_pre_txn_time_ns, 5_500);
        assert_eq!(
            report.connection_ceremony.commit_txn_roundtrip_time_ns,
            6_600
        );
        assert_eq!(report.connection_ceremony.commit_finalize_seq_time_ns, 700);
        assert_eq!(
            report.connection_ceremony.commit_handle_finalize_time_ns,
            800
        );
        assert_eq!(
            report
                .connection_ceremony
                .commit_post_write_maintenance_time_ns,
            900
        );
        assert_eq!(
            report.connection_ceremony.finalize_post_publish_time_ns,
            1_250
        );
        assert_eq!(report.connection_ceremony.arena_alloc_bytes, 2_048);
        assert_eq!(report.connection_ceremony.arena_reset_count, 3);
        assert_eq!(
            report
                .connection_ceremony
                .fallback_to_general_allocator_count,
            1
        );

        let artifact_dir = tempdir.path().join("artifacts");
        let counter_capture_summary = HotPathCounterCaptureManifestSummary {
            host_capability_sensitive_captures: vec!["topdown".to_owned()],
            topology_sensitive_captures: vec!["cache_to_cache".to_owned()],
            fallback_tools: vec!["perf-stat".to_owned()],
            fallback_metric_pack: vec!["cache-misses".to_owned()],
            fallback_notes: vec!["cache_to_cache: perf c2c unavailable".to_owned()],
            raw_output_relpaths: vec!["profiles/perf-c2c.profiler_safe.data".to_owned()],
        };
        let provenance = HotPathArtifactProvenance {
            row_id: "hot_page_contention_c4".to_owned(),
            mode_id: "fsqlite_mvcc".to_owned(),
            artifact_root: artifact_dir.display().to_string(),
            command_entrypoint: report.replay_command.clone(),
            workspace_root: Some(tempdir.path().display().to_string()),
            campaign_manifest_path: Some(
                "sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.json".to_owned(),
            ),
            source_revision: Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
            beads_data_hash: Some("a".repeat(64)),
            kernel_release: "Linux 6.13.5-test".to_owned(),
            rustc_version: "rustc 1.91.0-nightly".to_owned(),
            cargo_profile: "release-perf".to_owned(),
            commands: vec![crate::fixture_select::BenchmarkArtifactCommand {
                tool: "realdb-e2e".to_owned(),
                command_line: report.replay_command.clone(),
            }],
            tool_versions: vec![crate::fixture_select::BenchmarkArtifactToolVersion {
                tool: "cargo".to_owned(),
                version: "cargo 1.91.0-nightly".to_owned(),
            }],
            fallback_notes: counter_capture_summary.fallback_notes.clone(),
        };
        let microarchitectural_context = HotPathMicroarchitecturalContext {
            fixture_id: report.fixture_id.clone(),
            row_id: provenance.row_id.clone(),
            mode_id: provenance.mode_id.clone(),
            placement_profile_id: Some("baseline_unpinned".to_owned()),
            hardware_class_id: Some("linux_x86_64_any".to_owned()),
            hardware_signature: Some("linux:x86_64:any".to_owned()),
        };
        let manifest = write_hot_path_profile_artifacts(
            &report,
            &artifact_dir,
            Some(counter_capture_summary.clone()),
            Some(provenance.clone()),
            Some(microarchitectural_context.clone()),
        )
        .unwrap();
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
            manifest.counter_capture_summary,
            Some(counter_capture_summary.clone())
        );
        assert_eq!(manifest.provenance, Some(provenance.clone()));
        let disk_manifest: HotPathArtifactManifest =
            serde_json::from_slice(&std::fs::read(artifact_dir.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(
            disk_manifest.counter_capture_summary,
            Some(counter_capture_summary)
        );
        assert_eq!(disk_manifest.provenance, Some(provenance));
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
            HOT_PATH_PROFILE_ACTIONABLE_RANKING_SCHEMA_V3
        );
        assert!(!opcode_profile.opcodes.is_empty());
        assert!(!subsystem_profile.subsystem_ranking.is_empty());
        assert_eq!(
            subsystem_profile.btree_copy_kernel_targets,
            report.btree_copy_kernel_targets
        );
        assert!(
            subsystem_profile
                .btree_copy_kernels
                .overflow_chain_overflow_bytes
                > 0
        );
        assert!(subsystem_profile.mvcc_write.page_lock_waits_total > 0);
        assert_eq!(subsystem_profile.page_buffer_pool, report.page_buffer_pool);
        assert!(subsystem_profile.page_data_motion.normalized_bytes_total > 0);
        assert!(!actionable_ranking.baseline_reuse_ledger.is_empty());
        assert!(!actionable_ranking.baseline_waste_ledger.is_empty());
        assert!(!actionable_ranking.named_hotspots.is_empty());
        assert_eq!(actionable_ranking.microarchitectural_signatures.len(), 8);
        assert_eq!(actionable_ranking.wall_time_components.len(), 8);
        assert_eq!(actionable_ranking.causal_buckets.len(), 7);
        assert!(!actionable_ranking.cost_components.is_empty());
        assert_eq!(actionable_ranking.allocator_pressure.len(), 4);
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
                .contains("## B-Tree Copy Kernel Targets")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Parser Reuse")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("`compile_reuse_count`")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("`statement_cache_hit` / `statement_cache_miss`")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("`plan_cache_hit` / `plan_cache_miss`")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("`invalidation_reason`")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Record Decode")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("`record_decodes_per_row`")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Connection Ceremony")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("Statement lookaside alloc/reset/fallback")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Baseline Reuse Ledger")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## MVCC Write Path")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Page Buffer Pool")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("`page_buffer_pool_hit` / `page_buffer_pool_miss`")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("`page_image_slab_reuse_count`")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("`staged_page_copy_bytes`")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## PageData Motion")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("in_place_zero_extends")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Baseline Waste Ledger")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Wall-Time Decomposition")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Causal Classification")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Mandatory Perf Checklist")
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("fixture `smoke`, workload `hot_page_contention`, concurrency `c1`")
        );
        assert!(
            actionable_ranking
                .named_hotspots
                .iter()
                .flat_map(|entry| entry.mapped_beads.iter())
                .any(|bead| bead == "bd-db300.10.2"
                    || bead == "bd-db300.5.2.1"
                    || bead == "bd-db300.5.5.1"
                    || bead == "bd-db300.10.3"
                    || bead == "bd-db300.10.4"
                    || bead == "bd-db300.10.5"
                    || bead == "bd-db300.10.6"
                    || bead == "bd-db300.10.7")
        );
        assert_eq!(actionable_ranking.cost_components.len(), 4);
        let wall_component_names = actionable_ranking
            .wall_time_components
            .iter()
            .map(|entry| entry.component.as_str())
            .collect::<Vec<_>>();
        assert!(wall_component_names.contains(&"queueing"));
        assert!(wall_component_names.contains(&"synchronization"));
        assert!(wall_component_names.contains(&"retry"));
        assert!(wall_component_names.contains(&"service"));
        assert!(wall_component_names.contains(&"allocator_copy"));
        assert!(wall_component_names.contains(&"durability"));
        assert!(wall_component_names.contains(&"mvcc_wait"));
        assert!(wall_component_names.contains(&"mvcc_commit_surface"));
        let causal_bucket_names = actionable_ranking
            .causal_buckets
            .iter()
            .map(|entry| entry.bucket.as_str())
            .collect::<Vec<_>>();
        assert!(causal_bucket_names.contains(&"service"));
        assert!(causal_bucket_names.contains(&"queueing"));
        assert!(causal_bucket_names.contains(&"synchronization"));
        assert!(causal_bucket_names.contains(&"allocation"));
        assert!(causal_bucket_names.contains(&"io"));
        assert!(causal_bucket_names.contains(&"retries"));
        assert!(causal_bucket_names.contains(&"mixed"));
        assert_eq!(
            actionable_ranking
                .causal_buckets
                .iter()
                .filter(|entry| entry.dominant)
                .count(),
            1
        );
        assert_eq!(
            actionable_ranking.causal_classification.dominant_bucket,
            "synchronization"
        );
        assert_eq!(
            actionable_ranking
                .causal_buckets
                .first()
                .map(|entry| entry.bucket.as_str()),
            Some("synchronization")
        );
        assert_eq!(
            actionable_ranking
                .causal_buckets
                .first()
                .map(|entry| entry.dominant),
            Some(true)
        );
        assert_eq!(
            actionable_ranking
                .causal_classification
                .runner_up_bucket
                .as_deref(),
            Some("queueing")
        );
        assert_eq!(
            actionable_ranking
                .causal_classification
                .runner_up_gap_basis_points,
            Some(5_288)
        );
        assert!(!actionable_ranking.causal_classification.mixed_or_ambiguous);
        assert!(
            actionable_ranking
                .microarchitectural_signatures
                .iter()
                .all(|entry| entry.fixture_id == report.fixture_id)
        );
        assert!(
            actionable_ranking
                .microarchitectural_signatures
                .iter()
                .all(|entry| entry.row_id.as_deref()
                    == Some(microarchitectural_context.row_id.as_str()))
        );
        assert!(
            actionable_ranking
                .microarchitectural_signatures
                .iter()
                .all(|entry| entry.mode_id.as_deref()
                    == Some(microarchitectural_context.mode_id.as_str()))
        );
        assert!(
            actionable_ranking
                .microarchitectural_signatures
                .iter()
                .all(
                    |entry| entry.placement_profile_id.as_deref() == Some("baseline_unpinned")
                        && entry.hardware_class_id.as_deref() == Some("linux_x86_64_any")
                        && entry.hardware_signature.as_deref() == Some("linux:x86_64:any")
                        && !entry.evidence_sources.is_empty()
                )
        );
        let component_names = actionable_ranking
            .cost_components
            .iter()
            .map(|entry| entry.component.as_str())
            .collect::<Vec<_>>();
        assert!(component_names.contains(&"parser_ast_churn"));
        assert!(component_names.contains(&"record_decode"));
        assert!(component_names.contains(&"row_materialization"));
        assert!(component_names.contains(&"page_data_motion"));
        assert!(
            actionable_ranking
                .allocator_pressure
                .iter()
                .all(|entry| !entry.implication.is_empty() && !entry.mapped_beads.is_empty())
        );

        let parser_component = actionable_ranking
            .cost_components
            .iter()
            .find(|entry| entry.component == "parser_ast_churn")
            .unwrap();
        assert_eq!(
            parser_component.mapped_beads,
            vec!["bd-db300.10.2".to_owned(), "bd-db300.10.4".to_owned()]
        );
        assert!(!parser_component.implication.is_empty());

        let record_decode_component = actionable_ranking
            .cost_components
            .iter()
            .find(|entry| entry.component == "record_decode")
            .unwrap();
        assert_eq!(
            record_decode_component.mapped_beads,
            vec!["bd-db300.10.2".to_owned(), "bd-db300.10.5".to_owned()]
        );
        assert!(!record_decode_component.implication.is_empty());

        let row_materialization_component = actionable_ranking
            .cost_components
            .iter()
            .find(|entry| entry.component == "row_materialization")
            .unwrap();
        assert_eq!(
            row_materialization_component.mapped_beads,
            vec![
                "bd-db300.10.2".to_owned(),
                "bd-db300.10.6".to_owned(),
                "bd-db300.10.7".to_owned(),
            ]
        );
        assert!(!row_materialization_component.implication.is_empty());

        let page_data_motion_component = actionable_ranking
            .cost_components
            .iter()
            .find(|entry| entry.component == "page_data_motion")
            .unwrap();
        assert_eq!(
            page_data_motion_component.mapped_beads,
            vec!["bd-db300.10.3".to_owned(), "bd-db300.10.6".to_owned()]
        );
        assert!(!page_data_motion_component.implication.is_empty());

        let parse_cache_reuse = actionable_ranking
            .baseline_reuse_ledger
            .iter()
            .find(|entry| entry.surface == "statement_parse_cache")
            .unwrap();
        assert!(parse_cache_reuse.supported);
        assert_eq!(parse_cache_reuse.hit_rate_basis_points, Some(5_000));
        assert_eq!(
            parse_cache_reuse.mapped_beads,
            vec!["bd-db300.10.4".to_owned()]
        );

        let cursor_frame_reuse = actionable_ranking
            .baseline_reuse_ledger
            .iter()
            .find(|entry| entry.surface == "cursor_frame_reuse")
            .unwrap();
        assert!(cursor_frame_reuse.supported);
        assert_eq!(cursor_frame_reuse.hit_rate_basis_points, Some(5_000));
        assert_eq!(
            cursor_frame_reuse.mapped_beads,
            vec!["bd-db300.10.7".to_owned()]
        );

        let record_decode_cache = actionable_ranking
            .baseline_reuse_ledger
            .iter()
            .find(|entry| entry.surface == "record_decode_cache")
            .unwrap();
        assert!(record_decode_cache.supported);
        assert_eq!(record_decode_cache.hits, 4);
        assert_eq!(record_decode_cache.misses, 3);
        assert_eq!(record_decode_cache.hit_rate_basis_points, Some(5_714));
        assert_eq!(
            record_decode_cache.mapped_beads,
            vec!["bd-db300.10.5".to_owned()]
        );

        let page_data_reuse = actionable_ranking
            .baseline_reuse_ledger
            .iter()
            .find(|entry| entry.surface == "page_data_ownership_reuse")
            .unwrap();
        assert!(page_data_reuse.supported);
        assert_eq!(page_data_reuse.hit_rate_basis_points, Some(8_000));
        assert_eq!(
            page_data_reuse.mapped_beads,
            vec!["bd-db300.10.6".to_owned()]
        );

        let page_buffer_pool_reuse = actionable_ranking
            .baseline_reuse_ledger
            .iter()
            .find(|entry| entry.surface == "page_buffer_pool_reuse")
            .unwrap();
        assert!(page_buffer_pool_reuse.supported);
        assert_eq!(page_buffer_pool_reuse.hits, 3);
        assert_eq!(page_buffer_pool_reuse.misses, 1);
        assert_eq!(page_buffer_pool_reuse.hit_rate_basis_points, Some(7_500));
        assert_eq!(
            page_buffer_pool_reuse.mapped_beads,
            vec!["bd-db300.10.3".to_owned()]
        );

        let parser_waste = actionable_ranking
            .baseline_waste_ledger
            .iter()
            .find(|entry| entry.component == "parser_prepare_churn")
            .unwrap();
        assert_eq!(parser_waste.classification, "baseline_tax");
        assert_eq!(parser_waste.metric_kind, "time_ns");
        assert_eq!(
            parser_waste.mapped_beads,
            vec!["bd-db300.10.2".to_owned(), "bd-db300.10.4".to_owned()]
        );

        let busy_retry_spillover = actionable_ranking
            .baseline_waste_ledger
            .iter()
            .find(|entry| entry.component == "busy_retry_queueing")
            .unwrap();
        assert_eq!(
            busy_retry_spillover.classification,
            "structural_side_effect"
        );
        assert_eq!(busy_retry_spillover.metric_value, 2_000_000);
        assert_eq!(
            busy_retry_spillover.mapped_beads,
            vec!["bd-db300.2.4".to_owned()]
        );

        let mvcc_wait = actionable_ranking
            .baseline_waste_ledger
            .iter()
            .find(|entry| entry.component == "mvcc_page_lock_wait")
            .unwrap();
        assert_eq!(mvcc_wait.classification, "structural_side_effect");
        assert_eq!(mvcc_wait.metric_value, 2_400_000);
        assert_eq!(
            mvcc_wait.mapped_beads,
            vec!["bd-db300.5.2.1".to_owned(), "bd-db300.5.5.1".to_owned()]
        );

        let page_data_normalization = actionable_ranking
            .baseline_waste_ledger
            .iter()
            .find(|entry| entry.component == "page_data_normalization")
            .unwrap();
        assert_eq!(page_data_normalization.classification, "baseline_tax");
        assert_eq!(page_data_normalization.metric_kind, "bytes");
        assert_eq!(page_data_normalization.metric_value, 1_504);
        assert_eq!(
            page_data_normalization.mapped_beads,
            vec!["bd-db300.10.3".to_owned(), "bd-db300.10.6".to_owned()]
        );

        let synchronization_bucket = actionable_ranking
            .causal_buckets
            .iter()
            .find(|entry| entry.bucket == "synchronization")
            .unwrap();
        assert!(synchronization_bucket.dominant);
        assert_eq!(synchronization_bucket.estimated_time_ns, 4_800_000);
        assert_eq!(synchronization_bucket.score_basis_points, 7_051);
        assert!(synchronization_bucket.evidence.iter().any(|evidence| {
            evidence.metric_path
                == ".wall_time_components[] | select(.component == \"mvcc_wait\") | .time_ns"
        }));
        assert!(synchronization_bucket.evidence.iter().any(|evidence| {
            evidence.metric_path == ".engine_report.hot_path_profile.vfs.lock_ops"
        }));

        let service_bucket = actionable_ranking
            .causal_buckets
            .iter()
            .find(|entry| entry.bucket == "service")
            .unwrap();
        assert_eq!(service_bucket.estimated_time_ns, 5_000);
        assert!(service_bucket.evidence.iter().any(|evidence| {
            evidence.metric_path == ".engine_report.runtime_phase_timing.body_execution_time_ns"
        }));

        let allocation_bucket = actionable_ranking
            .causal_buckets
            .iter()
            .find(|entry| entry.bucket == "allocation")
            .unwrap();
        assert_eq!(allocation_bucket.estimated_time_ns, 2_500);
        assert!(allocation_bucket.evidence.iter().any(|evidence| {
            evidence.metric_path
                == ".baseline_waste_ledger[] | select(.component == \"page_data_normalization\") | .metric_value"
        }));

        let io_bucket = actionable_ranking
            .causal_buckets
            .iter()
            .find(|entry| entry.bucket == "io")
            .unwrap();
        assert_eq!(io_bucket.estimated_time_ns, 0);
        assert!(io_bucket.evidence.iter().any(|evidence| {
            evidence.metric_path == ".engine_report.hot_path_profile.wal.bytes_written_total"
        }));

        let retries_bucket = actionable_ranking
            .causal_buckets
            .iter()
            .find(|entry| entry.bucket == "retries")
            .unwrap();
        assert_eq!(retries_bucket.estimated_time_ns, 800_000);
        assert!(retries_bucket.evidence.iter().any(|evidence| {
            evidence.metric_path == ".engine_report.runtime_phase_timing.retry_backoff_time_ns"
        }));
    }

    #[test]
    fn hot_path_profile_smoke_respects_bead_override() {
        let config = FsqliteHotPathProfileConfig {
            workload: "hot_page_contention".to_owned(),
            seed: 7,
            scale: 6,
            concurrency: 1,
            exec_config: crate::fsqlite_executor::FsqliteExecConfig {
                concurrent_mode: true,
                run_integrity_check: false,
                ..crate::fsqlite_executor::FsqliteExecConfig::default()
            },
            replay_command:
                "cargo run -p fsqlite-e2e --bin realdb-e2e -- hot-profile --db smoke --workload hot_page_contention"
                    .to_owned(),
            golden_dir: Some("sample_sqlite_db_files/golden".to_owned()),
            working_base: Some("sample_sqlite_db_files/working".to_owned()),
            bead_id: Some("bd-db300.1.3".to_owned()),
            scenario_prefix: None,
        };

        let report = sample_hot_path_profile_report(&config);

        assert_eq!(report.bead_id, "bd-db300.1.3");
        assert_eq!(report.scenario_id, "bd-db300.1.3.hot_page_contention");
    }

    #[test]
    fn hot_path_profile_scope_rejects_reentrant_entry() {
        let _guard = hot_path_test_guard();
        reset_hot_path_profile();
        set_hot_path_profile_enabled(false);

        let first_scope = HotPathProfileScope::enable().unwrap();

        let second_scope = HotPathProfileScope::enable();
        assert!(
            second_scope.is_err(),
            "reentrant profiling scope should fail"
        );
        let Err(error) = second_scope else {
            return;
        };

        assert!(error.to_string().contains("already active in this process"));

        drop(first_scope);
        set_hot_path_profile_enabled(false);
        reset_hot_path_profile();
    }
}
