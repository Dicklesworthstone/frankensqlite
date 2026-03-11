use serde::{Deserialize, Serialize};

use crate::methodology::{EnvironmentCaptureMode, EnvironmentMeta, MethodologyMeta};

/// JSON schema version for the E2E report format.
///
/// This is a human-readable version string intended for `report.json` consumers.
pub const REPORT_SCHEMA_V1: &str = "fsqlite-e2e.report.v1";

/// JSON schema version for per-run JSONL records.
///
/// Each JSONL line should contain exactly one [`RunRecordV1`] object.
pub const RUN_RECORD_SCHEMA_V1: &str = "fsqlite-e2e.run_record.v1";
/// JSON schema version for opt-in FrankenSQLite hot-path profile records.
pub const HOT_PATH_PROFILE_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_profile.v1";

/// Human-readable explanation of the RealDB E2E equality policy tiers.
///
/// This string is duplicated into each report so JSON consumers don't have to
/// hardcode the meaning of each tier.
pub const EQUALITY_POLICY_EXPLANATION_V1: &str = "\
FrankenSQLite RealDB E2E equality tiers (best-effort):\n\
\n\
1) raw_sha256\n\
   - Meaning: SHA-256 of the raw on-disk database bytes as produced by each engine.\n\
   - Use: Strict diagnostic signal.\n\
   - Caveat: Expected to differ even for logically identical DBs due to page layout,\n\
     freelist state, and WAL/shm/journal sidecars.\n\
\n\
2) canonical_sha256\n\
   - Meaning: SHA-256 after a deterministic canonicalization step (e.g. checkpoint + VACUUM INTO\n\
     a fresh database file).\n\
   - Use: Intended default compatibility proof when available.\n\
\n\
3) logical\n\
   - Meaning: Compare logical content via deterministic validation queries (e.g. schema + table\n\
     rows with stable ordering) and require PRAGMA integrity_check to return ok on both engines.\n\
   - Use: Fallback when canonicalization is unavailable or mismatches.\n\
";

/// Top-level report for a single E2E run.
///
/// A run may contain multiple benchmark/correctness cases (fixture × workload × concurrency).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eReport {
    pub schema_version: String,
    pub run: RunInfo,
    /// Benchmark methodology that governs how measurements were taken.
    pub methodology: MethodologyMeta,
    /// Environment snapshot captured at benchmark time for reproducibility.
    pub environment: EnvironmentMeta,
    pub fixture: FixtureInfo,
    pub workload: WorkloadInfo,
    pub cases: Vec<CaseReport>,
}

impl E2eReport {
    pub fn new(
        run: RunInfo,
        fixture: FixtureInfo,
        workload: WorkloadInfo,
        environment: EnvironmentMeta,
    ) -> Self {
        Self {
            schema_version: REPORT_SCHEMA_V1.to_owned(),
            run,
            methodology: MethodologyMeta::current(),
            environment,
            fixture,
            workload,
            cases: Vec::new(),
        }
    }
}

/// Explicit run-mode behavior carried by machine-readable run records.
///
/// This lets downstream tools distinguish full-validation runs from lower-
/// overhead profiler-safe runs without reverse-engineering the meaning of a
/// suppressed environment block and nullable integrity-check result.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RunModeProvenance {
    /// True when the serialized behavior matches `--profile-only`, either via
    /// the explicit flag or an equivalent pair of granular skip flags.
    pub profile_only_effective: bool,
    /// Whether the run performed post-run `PRAGMA integrity_check`.
    pub run_integrity_check: bool,
    /// Whether the run captured benchmark environment metadata.
    pub capture_environment_metadata: bool,
}

impl RunModeProvenance {
    #[must_use]
    pub fn from_environment_and_report(
        environment: &EnvironmentMeta,
        report: &EngineRunReport,
    ) -> Self {
        let capture_environment_metadata =
            environment.capture_mode == EnvironmentCaptureMode::Captured;
        let run_integrity_check = report.correctness.integrity_check_ok.is_some();
        Self {
            profile_only_effective: !capture_environment_metadata && !run_integrity_check,
            run_integrity_check,
            capture_environment_metadata,
        }
    }
}

/// A single JSONL record for a single-engine run.
///
/// This is intentionally a "flat" record suitable for append-only JSONL logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecordV1 {
    pub schema_version: String,
    /// Milliseconds since Unix epoch, captured when the record is written.
    pub recorded_unix_ms: u64,
    /// Benchmark methodology that governs how measurements were taken.
    pub methodology: MethodologyMeta,
    /// Environment snapshot captured at benchmark time for reproducibility.
    pub environment: EnvironmentMeta,
    /// Explicit run-mode behavior derived from the serialized record contents.
    #[serde(default)]
    pub run_mode: RunModeProvenance,
    pub engine: EngineInfo,
    pub fixture_id: String,
    pub golden_path: Option<String>,
    /// SHA-256 of the golden input DB file, if known.
    pub golden_sha256: Option<String>,
    pub workload: String,
    pub concurrency: u16,
    pub ops_count: u64,
    pub report: EngineRunReport,
}

/// Constructor parameters for [`RunRecordV1`].
#[derive(Debug, Clone)]
pub struct RunRecordV1Args {
    pub recorded_unix_ms: u64,
    pub environment: EnvironmentMeta,
    pub engine: EngineInfo,
    pub fixture_id: String,
    pub golden_path: Option<String>,
    pub golden_sha256: Option<String>,
    pub workload: String,
    pub concurrency: u16,
    pub ops_count: u64,
    pub report: EngineRunReport,
}

impl RunRecordV1 {
    #[must_use]
    pub fn new(args: RunRecordV1Args) -> Self {
        let run_mode =
            RunModeProvenance::from_environment_and_report(&args.environment, &args.report);
        Self {
            schema_version: RUN_RECORD_SCHEMA_V1.to_owned(),
            recorded_unix_ms: args.recorded_unix_ms,
            methodology: MethodologyMeta::current(),
            environment: args.environment,
            run_mode,
            engine: args.engine,
            fixture_id: args.fixture_id,
            golden_path: args.golden_path,
            golden_sha256: args.golden_sha256,
            workload: args.workload,
            concurrency: args.concurrency,
            ops_count: args.ops_count,
            report: args.report,
        }
    }

    pub fn to_jsonl_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn to_pretty_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Standalone hot-path profile artifact for a single FrankenSQLite run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotPathProfileRecordV1 {
    pub schema_version: String,
    /// Milliseconds since Unix epoch, captured when the profile is written.
    pub recorded_unix_ms: u64,
    pub fixture_id: String,
    pub golden_path: Option<String>,
    pub golden_sha256: Option<String>,
    pub workload: String,
    pub concurrency: u16,
    pub concurrent_mode: bool,
    pub ops_count: u64,
    pub report: EngineRunReport,
    pub profile: FsqliteHotPathProfile,
}

/// Constructor parameters for [`HotPathProfileRecordV1`].
#[derive(Debug, Clone)]
pub struct HotPathProfileRecordV1Args {
    pub recorded_unix_ms: u64,
    pub fixture_id: String,
    pub golden_path: Option<String>,
    pub golden_sha256: Option<String>,
    pub workload: String,
    pub concurrency: u16,
    pub concurrent_mode: bool,
    pub ops_count: u64,
    pub report: EngineRunReport,
    pub profile: FsqliteHotPathProfile,
}

impl HotPathProfileRecordV1 {
    #[must_use]
    pub fn new(args: HotPathProfileRecordV1Args) -> Self {
        Self {
            schema_version: HOT_PATH_PROFILE_SCHEMA_V1.to_owned(),
            recorded_unix_ms: args.recorded_unix_ms,
            fixture_id: args.fixture_id,
            golden_path: args.golden_path,
            golden_sha256: args.golden_sha256,
            workload: args.workload,
            concurrency: args.concurrency,
            concurrent_mode: args.concurrent_mode,
            ops_count: args.ops_count,
            report: args.report,
            profile: args.profile,
        }
    }

    pub fn to_jsonl_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn to_pretty_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Opt-in FrankenSQLite-only hot-path profile payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FsqliteHotPathProfile {
    /// How opcode-level attribution was collected.
    pub collection_mode: String,
    pub parser: ParserHotPathProfile,
    pub vdbe: VdbeHotPathProfile,
    pub vfs: VfsHotPathProfile,
    pub wal: WalHotPathProfile,
    pub decoded_values: HotPathValueHistogram,
    pub workload_input_types: HotPathValueHistogram,
    pub result_rows: ResultRowHotPathProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocator_pressure: Option<AllocatorPressureHotPathProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btree: Option<BtreeRuntimeHotPathProfile>,
    pub statement_hotspots: Vec<StatementHotspot>,
}

/// Parser-side churn observed during the profiled run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ParserHotPathProfile {
    pub tokenize_tokens_total: u64,
    pub tokenize_calls_total: u64,
    pub tokenize_duration_sum_micros: u64,
    pub parsed_statements_total: u64,
    pub semantic_errors_total: u64,
}

/// Estimated opcode mix plus actual VDBE aggregate counters.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct VdbeHotPathProfile {
    pub actual_opcodes_executed_total: u64,
    pub actual_statements_total: u64,
    pub actual_statement_duration_us_total: u64,
    pub actual_sort_rows_total: u64,
    pub actual_sort_spill_pages_total: u64,
    pub actual_column_reads_total: u64,
    pub actual_record_decode_calls_total: u64,
    pub actual_decoded_values_total: u64,
    pub actual_decoded_value_heap_bytes_total: u64,
    pub actual_make_record_calls_total: u64,
    pub actual_make_record_blob_bytes_total: u64,
    pub actual_type_coercions_total: u64,
    pub actual_type_coercion_changes_total: u64,
    /// Explain-derived weighted estimate: prepared bytecode shape multiplied by
    /// observed statement execution counts.
    pub estimated_total_opcodes: u64,
    pub estimated_column_opcodes_total: u64,
    pub estimated_make_record_opcodes_total: u64,
    pub estimated_result_row_opcodes_total: u64,
    pub estimated_unattributed_statement_executions_total: u64,
    #[serde(default)]
    pub top_actual_opcodes: Vec<HotPathOpcodeCount>,
    pub top_estimated_opcodes: Vec<HotPathOpcodeCount>,
}

/// Weighted opcode count entry used in hot-path reports.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HotPathOpcodeCount {
    pub opcode: String,
    pub estimated_count: u64,
}

/// Type and byte histogram for row/value materialization.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HotPathValueHistogram {
    pub nulls: u64,
    pub integers: u64,
    pub reals: u64,
    pub texts: u64,
    pub blobs: u64,
    pub text_bytes_total: u64,
    pub blob_bytes_total: u64,
}

/// Result-row materialization observed through `trace_v2` row callbacks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ResultRowHotPathProfile {
    pub rows_total: u64,
    pub values_total: u64,
    pub value_types: HotPathValueHistogram,
}

/// Statement-level hotspot summary derived from `trace_v2` profile callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct StatementHotspot {
    pub sql: String,
    pub execution_count: u64,
    pub total_elapsed_ns: u64,
    pub max_elapsed_ns: u64,
}

/// Estimated allocator pressure observed during the profiled run.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AllocatorPressureHotPathProfile {
    pub estimated_heap_bytes_total: u64,
    pub dominant_sources: Vec<HotPathOpcodeCount>,
    pub ranked_hotspots: Vec<HotPathEvidence>,
}

/// B-tree runtime counters observed during the profiled run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct BtreeRuntimeHotPathProfile {
    pub seek_total: u64,
    pub insert_total: u64,
    pub delete_total: u64,
    pub page_splits_total: u64,
    pub swiss_probes_total: u64,
    pub swizzle_faults_total: u64,
    pub swizzle_in_total: u64,
    pub swizzle_out_total: u64,
}

/// Ranked hotspot/evidence entry for perf triage.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HotPathEvidence {
    pub label: String,
    pub value: u64,
    pub detail: String,
}

/// VFS delta observed during the profiled run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct VfsHotPathProfile {
    pub read_ops: u64,
    pub write_ops: u64,
    pub sync_ops: u64,
    pub lock_ops: u64,
    pub unlock_ops: u64,
    pub truncate_ops: u64,
    pub close_ops: u64,
    pub file_size_ops: u64,
    pub read_bytes_total: u64,
    pub write_bytes_total: u64,
}

/// WAL-side delta observed during the profiled run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct WalHotPathProfile {
    pub frames_written_total: u64,
    pub bytes_written_total: u64,
    pub checkpoint_count: u64,
    pub checkpoint_frames_backfilled_total: u64,
    pub checkpoint_duration_us_total: u64,
    pub wal_resets_total: u64,
    pub group_commits_total: u64,
    pub group_commit_size_sum: u64,
    pub group_commit_latency_us_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineInfo {
    pub name: String,
    /// SQLite version string for the sqlite3/rusqlite oracle, if applicable.
    pub sqlite_version: Option<String>,
    /// Git metadata for FrankenSQLite, if applicable.
    pub fsqlite_git: Option<GitInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunInfo {
    /// Stable identifier for correlating logs/artifacts across steps.
    pub run_id: String,
    /// Milliseconds since Unix epoch, captured at run start.
    pub started_unix_ms: u64,
    /// Milliseconds since Unix epoch, captured at run finish (if finished).
    pub finished_unix_ms: Option<u64>,
    /// Optional git metadata for reproducibility.
    pub git: Option<GitInfo>,
    /// Optional host metadata for reproducibility.
    pub host: Option<HostInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitInfo {
    pub commit: String,
    pub dirty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostInfo {
    pub os: String,
    pub arch: String,
    pub cpu_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureInfo {
    pub fixture_id: String,
    pub bucket: FixtureBucket,
    /// Absolute path to the source DB (outside the repo), if known.
    pub source_path: Option<String>,
    /// Path to the golden copy within the repo's fixture corpus, if present.
    pub golden_path: Option<String>,
    /// Path to the working copy used for this run, if present.
    pub working_path: Option<String>,
    pub size_bytes: u64,
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureBucket {
    Small,
    Medium,
    Large,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadInfo {
    pub preset: String,
    pub seed: u64,
    pub rng: RngInfo,
    /// Rows per transaction (or other workload-defined unit), if applicable.
    pub transaction_size: Option<u32>,
    /// If the workload requires explicit commit ordering for determinism, record the policy here.
    pub commit_order_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RngInfo {
    pub algorithm: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseReport {
    pub case_id: String,
    pub concurrency: u16,
    pub sqlite3: EngineRunReport,
    pub fsqlite: EngineRunReport,
    pub comparison: Option<ComparisonReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineRunReport {
    pub wall_time_ms: u64,
    pub ops_total: u64,
    pub ops_per_sec: f64,
    pub retries: u64,
    pub aborts: u64,
    pub correctness: CorrectnessReport,
    pub latency_ms: Option<LatencySummary>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hot_path_profile: Option<FsqliteHotPathProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySummary {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrectnessReport {
    /// Tier 1: strict SHA-256 match of the raw (non-canonicalized) database bytes.
    ///
    /// This is *not* the default compatibility criterion: two engines can produce
    /// identical logical content while yielding different byte layouts (page
    /// allocation, freelists, WAL/checkpoint state, etc.).
    ///
    /// Intended primarily as a "did we literally write the same bytes?" check
    /// after ensuring the DB has been checkpointed/flushed.
    pub raw_sha256_match: Option<bool>,
    pub dump_match: Option<bool>,
    pub canonical_sha256_match: Option<bool>,
    /// Best-effort: whether `PRAGMA integrity_check` returned "ok".
    pub integrity_check_ok: Option<bool>,
    /// Best-effort: SHA-256 of raw database bytes for this engine's output.
    pub raw_sha256: Option<String>,
    /// Best-effort: SHA-256 after canonicalization (e.g. VACUUM INTO).
    pub canonical_sha256: Option<String>,
    /// Best-effort: SHA-256 of a deterministic logical dump for this engine.
    pub logical_sha256: Option<String>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub verdict: ComparisonVerdict,
    pub tiers: EqualityTiersReport,
    pub explanation: String,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EqualityTiersReport {
    pub raw_sha256_match: Option<bool>,
    pub canonical_sha256_match: Option<bool>,
    pub logical_match: Option<bool>,
}

impl EqualityTiersReport {
    #[must_use]
    pub fn derive(sqlite3: &CorrectnessReport, fsqlite: &CorrectnessReport) -> Self {
        fn opt_eq(a: Option<&String>, b: Option<&String>) -> Option<bool> {
            match (a, b) {
                (Some(a), Some(b)) => Some(a == b),
                _ => None,
            }
        }

        let raw_sha256_match = opt_eq(sqlite3.raw_sha256.as_ref(), fsqlite.raw_sha256.as_ref());
        let canonical_sha256_match = opt_eq(
            sqlite3.canonical_sha256.as_ref(),
            fsqlite.canonical_sha256.as_ref(),
        );

        let integrity_both_ok = match (sqlite3.integrity_check_ok, fsqlite.integrity_check_ok) {
            (Some(a), Some(b)) => Some(a && b),
            _ => None,
        };
        let logical_sha_match = opt_eq(
            sqlite3.logical_sha256.as_ref(),
            fsqlite.logical_sha256.as_ref(),
        );
        let logical_match = match (logical_sha_match, integrity_both_ok) {
            (Some(true), Some(true)) => Some(true),
            (Some(false), _) | (_, Some(false)) => Some(false),
            _ => None,
        };

        Self {
            raw_sha256_match,
            canonical_sha256_match,
            logical_match,
        }
    }
}

impl ComparisonReport {
    /// Derive a full comparison report from two engine correctness reports.
    ///
    /// The verdict is determined by the equality policy tiers in priority order:
    ///   1. **canonical_sha256**: the intended default compatibility proof.
    ///      If both engines produce canonical hashes and they match, the verdict
    ///      is `Match` regardless of the raw tier.
    ///   2. **logical**: fallback when canonicalization is unavailable.
    ///      Requires both `integrity_check ok` and matching logical SHA-256.
    ///   3. **raw_sha256**: informational only — raw byte equality is not required
    ///      for a `Match` verdict because page layout legitimately differs between
    ///      engines even for logically identical databases.
    ///
    /// A `Mismatch` verdict is produced when the canonical tier explicitly
    /// mismatches, or (if canonical is unavailable) when the logical tier
    /// explicitly mismatches.  If neither tier is computable, the verdict
    /// is `Error` (insufficient data).
    #[must_use]
    pub fn derive(sqlite3: &CorrectnessReport, fsqlite: &CorrectnessReport) -> Self {
        let tiers = EqualityTiersReport::derive(sqlite3, fsqlite);
        let (verdict, explanation) = Self::verdict_and_explanation(&tiers);
        Self {
            verdict,
            tiers,
            explanation,
            notes: None,
        }
    }

    fn verdict_and_explanation(tiers: &EqualityTiersReport) -> (ComparisonVerdict, String) {
        // Canonical tier takes priority.
        if let Some(canonical) = tiers.canonical_sha256_match {
            if canonical {
                return (
                    ComparisonVerdict::Match,
                    "canonical_sha256 match: both engines produced identical \
                     post-VACUUM database files."
                        .to_owned(),
                );
            }
            return (
                ComparisonVerdict::Mismatch,
                "canonical_sha256 MISMATCH: database files differ after \
                 canonicalization (checkpoint + VACUUM INTO)."
                    .to_owned(),
            );
        }

        // Logical tier is the fallback.
        if let Some(logical) = tiers.logical_match {
            if logical {
                return (
                    ComparisonVerdict::Match,
                    "logical match: both engines pass integrity_check and \
                     produce identical logical dumps (canonical tier unavailable)."
                        .to_owned(),
                );
            }
            return (
                ComparisonVerdict::Mismatch,
                "logical MISMATCH: engines differ on logical content or \
                 integrity_check (canonical tier unavailable)."
                    .to_owned(),
            );
        }

        // Neither decisive tier is available.
        let mut msg =
            String::from("insufficient data: neither canonical nor logical tier is computable.");
        if let Some(raw) = tiers.raw_sha256_match {
            use std::fmt::Write;
            let _ = write!(
                msg,
                " (raw_sha256 {}, but raw equality is informational only.)",
                if raw { "matches" } else { "differs" }
            );
        }
        (ComparisonVerdict::Error, msg)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonVerdict {
    Match,
    Mismatch,
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::methodology::EnvironmentCaptureMode;

    fn cr(
        integrity_check_ok: Option<bool>,
        raw_sha256: Option<&str>,
        canonical_sha256: Option<&str>,
        logical_sha256: Option<&str>,
    ) -> CorrectnessReport {
        CorrectnessReport {
            raw_sha256_match: None,
            dump_match: None,
            canonical_sha256_match: None,
            integrity_check_ok,
            raw_sha256: raw_sha256.map(str::to_owned),
            canonical_sha256: canonical_sha256.map(str::to_owned),
            logical_sha256: logical_sha256.map(str::to_owned),
            notes: None,
        }
    }

    #[test]
    fn run_record_jsonl_roundtrip() {
        let report = EngineRunReport {
            wall_time_ms: 123,
            ops_total: 7,
            ops_per_sec: 3.5_f64,
            retries: 0,
            aborts: 0,
            correctness: cr(Some(true), None, None, None),
            latency_ms: None,
            error: None,
            hot_path_profile: None,
        };

        let record = RunRecordV1::new(RunRecordV1Args {
            recorded_unix_ms: 1_700_000_000_000,
            environment: crate::methodology::EnvironmentMeta::capture("test"),
            engine: EngineInfo {
                name: "sqlite3".to_owned(),
                sqlite_version: Some("3.52.0".to_owned()),
                fsqlite_git: None,
            },
            fixture_id: "fixture-a".to_owned(),
            golden_path: Some("/abs/golden.db".to_owned()),
            golden_sha256: Some("deadbeef".to_owned()),
            workload: "commutative_inserts_disjoint_keys".to_owned(),
            concurrency: 4,
            ops_count: 10,
            report,
        });

        let line = record.to_jsonl_line().unwrap();
        let parsed: RunRecordV1 = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed.schema_version, RUN_RECORD_SCHEMA_V1);
        assert_eq!(parsed.methodology.version, "fsqlite-e2e.methodology.v1");
        assert_eq!(
            parsed.environment.capture_mode,
            EnvironmentCaptureMode::Captured
        );
        assert!(!parsed.run_mode.profile_only_effective);
        assert!(parsed.run_mode.run_integrity_check);
        assert!(parsed.run_mode.capture_environment_metadata);
        assert!(!parsed.environment.arch.is_empty());
        assert_eq!(parsed.engine.name, "sqlite3");
        assert_eq!(parsed.concurrency, 4);
        assert_eq!(parsed.ops_count, 10);
        assert_eq!(parsed.report.wall_time_ms, 123);
    }

    #[test]
    fn run_record_marks_profile_only_equivalent_behavior_explicitly() {
        let report = EngineRunReport {
            wall_time_ms: 42,
            ops_total: 7,
            ops_per_sec: 3.5_f64,
            retries: 0,
            aborts: 0,
            correctness: cr(None, None, None, None),
            latency_ms: None,
            error: None,
            hot_path_profile: None,
        };

        let record = RunRecordV1::new(RunRecordV1Args {
            recorded_unix_ms: 1_700_000_000_000,
            environment: crate::methodology::EnvironmentMeta::suppressed("release-perf"),
            engine: EngineInfo {
                name: "fsqlite".to_owned(),
                sqlite_version: None,
                fsqlite_git: None,
            },
            fixture_id: "fixture-a".to_owned(),
            golden_path: Some("/abs/golden.db".to_owned()),
            golden_sha256: Some("deadbeef".to_owned()),
            workload: "mixed_read_write".to_owned(),
            concurrency: 8,
            ops_count: 10,
            report,
        });

        assert!(record.run_mode.profile_only_effective);
        assert!(!record.run_mode.run_integrity_check);
        assert!(!record.run_mode.capture_environment_metadata);

        let line = record.to_jsonl_line().unwrap();
        let parsed: RunRecordV1 = serde_json::from_str(&line).unwrap();
        assert!(parsed.run_mode.profile_only_effective);
        assert_eq!(
            parsed.environment.capture_mode,
            EnvironmentCaptureMode::Suppressed
        );
        assert_eq!(parsed.report.correctness.integrity_check_ok, None);
    }

    #[test]
    fn hot_path_profile_jsonl_roundtrip() {
        let report = EngineRunReport {
            wall_time_ms: 123,
            ops_total: 7,
            ops_per_sec: 3.5_f64,
            retries: 1,
            aborts: 2,
            correctness: cr(Some(true), None, None, None),
            latency_ms: None,
            error: None,
            hot_path_profile: None,
        };
        let profile = FsqliteHotPathProfile {
            collection_mode: "trace_v2+global_counters+explain_weighted_estimate".to_owned(),
            parser: ParserHotPathProfile {
                tokenize_tokens_total: 80,
                tokenize_calls_total: 4,
                tokenize_duration_sum_micros: 120,
                parsed_statements_total: 4,
                semantic_errors_total: 0,
            },
            vdbe: VdbeHotPathProfile {
                actual_opcodes_executed_total: 64,
                actual_statements_total: 4,
                actual_statement_duration_us_total: 90,
                actual_sort_rows_total: 0,
                actual_sort_spill_pages_total: 0,
                actual_column_reads_total: 8,
                actual_record_decode_calls_total: 4,
                actual_decoded_values_total: 12,
                actual_decoded_value_heap_bytes_total: 24,
                actual_make_record_calls_total: 2,
                actual_make_record_blob_bytes_total: 16,
                actual_type_coercions_total: 3,
                actual_type_coercion_changes_total: 1,
                estimated_total_opcodes: 72,
                estimated_column_opcodes_total: 8,
                estimated_make_record_opcodes_total: 4,
                estimated_result_row_opcodes_total: 2,
                estimated_unattributed_statement_executions_total: 0,
                top_actual_opcodes: vec![HotPathOpcodeCount {
                    opcode: "Column".to_owned(),
                    estimated_count: 8,
                }],
                top_estimated_opcodes: vec![HotPathOpcodeCount {
                    opcode: "Column".to_owned(),
                    estimated_count: 8,
                }],
            },
            vfs: VfsHotPathProfile {
                read_ops: 1,
                write_ops: 2,
                sync_ops: 0,
                lock_ops: 0,
                unlock_ops: 0,
                truncate_ops: 0,
                close_ops: 0,
                file_size_ops: 1,
                read_bytes_total: 4096,
                write_bytes_total: 8192,
            },
            wal: WalHotPathProfile {
                frames_written_total: 2,
                bytes_written_total: 8192,
                checkpoint_count: 0,
                checkpoint_frames_backfilled_total: 0,
                checkpoint_duration_us_total: 0,
                wal_resets_total: 0,
                group_commits_total: 1,
                group_commit_size_sum: 2,
                group_commit_latency_us_total: 50,
            },
            decoded_values: HotPathValueHistogram {
                integers: 9,
                texts: 3,
                text_bytes_total: 12,
                ..HotPathValueHistogram::default()
            },
            workload_input_types: HotPathValueHistogram {
                integers: 3,
                texts: 2,
                text_bytes_total: 9,
                ..HotPathValueHistogram::default()
            },
            result_rows: ResultRowHotPathProfile {
                rows_total: 2,
                values_total: 3,
                value_types: HotPathValueHistogram {
                    integers: 2,
                    texts: 1,
                    text_bytes_total: 3,
                    ..HotPathValueHistogram::default()
                },
            },
            allocator_pressure: Some(AllocatorPressureHotPathProfile {
                estimated_heap_bytes_total: 52,
                dominant_sources: vec![HotPathOpcodeCount {
                    opcode: "decoded_values".to_owned(),
                    estimated_count: 24,
                }],
                ranked_hotspots: vec![HotPathEvidence {
                    label: "record_decode".to_owned(),
                    value: 12,
                    detail: "12 decoded values across 4 decode calls".to_owned(),
                }],
            }),
            btree: Some(BtreeRuntimeHotPathProfile {
                seek_total: 5,
                insert_total: 1,
                delete_total: 0,
                page_splits_total: 0,
                swiss_probes_total: 7,
                swizzle_faults_total: 0,
                swizzle_in_total: 0,
                swizzle_out_total: 0,
            }),
            statement_hotspots: vec![StatementHotspot {
                sql: "SELECT 1;".to_owned(),
                execution_count: 2,
                total_elapsed_ns: 10,
                max_elapsed_ns: 7,
            }],
        };

        let record = HotPathProfileRecordV1::new(HotPathProfileRecordV1Args {
            recorded_unix_ms: 1_700_000_000_000,
            fixture_id: "fixture-a".to_owned(),
            golden_path: Some("/abs/golden.db".to_owned()),
            golden_sha256: Some("deadbeef".to_owned()),
            workload: "mixed_read_write".to_owned(),
            concurrency: 4,
            concurrent_mode: true,
            ops_count: 10,
            report,
            profile,
        });

        let line = record.to_jsonl_line().unwrap();
        let parsed: HotPathProfileRecordV1 = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed.schema_version, HOT_PATH_PROFILE_SCHEMA_V1);
        assert_eq!(parsed.fixture_id, "fixture-a");
        assert!(parsed.concurrent_mode);
        assert_eq!(parsed.profile.vdbe.estimated_total_opcodes, 72);
        assert_eq!(parsed.profile.statement_hotspots.len(), 1);
    }

    #[test]
    fn derive_tiers_raw_sha256_match() {
        let sqlite3 = cr(Some(true), Some("a"), None, None);
        let fsqlite = cr(Some(true), Some("a"), None, None);
        let tiers = EqualityTiersReport::derive(&sqlite3, &fsqlite);
        assert_eq!(tiers.raw_sha256_match, Some(true));
        assert_eq!(tiers.canonical_sha256_match, None);
        assert_eq!(tiers.logical_match, None);
    }

    #[test]
    fn derive_tiers_logical_match_requires_integrity_ok_and_hash_match() {
        // Hash match but one integrity_check unknown -> cannot assert logical match.
        let sqlite3 = cr(None, None, None, Some("h"));
        let fsqlite = cr(Some(true), None, None, Some("h"));
        let tiers = EqualityTiersReport::derive(&sqlite3, &fsqlite);
        assert_eq!(tiers.logical_match, None);

        // Hash match but integrity failure -> logical mismatch.
        let sqlite3 = cr(Some(false), None, None, Some("h"));
        let fsqlite = cr(Some(true), None, None, Some("h"));
        let tiers = EqualityTiersReport::derive(&sqlite3, &fsqlite);
        assert_eq!(tiers.logical_match, Some(false));

        // Integrity ok + hash mismatch -> logical mismatch.
        let sqlite3 = cr(Some(true), None, None, Some("h1"));
        let fsqlite = cr(Some(true), None, None, Some("h2"));
        let tiers = EqualityTiersReport::derive(&sqlite3, &fsqlite);
        assert_eq!(tiers.logical_match, Some(false));

        // Integrity ok + hash match -> logical match.
        let sqlite3 = cr(Some(true), None, None, Some("h"));
        let fsqlite = cr(Some(true), None, None, Some("h"));
        let tiers = EqualityTiersReport::derive(&sqlite3, &fsqlite);
        assert_eq!(tiers.logical_match, Some(true));
    }

    // --- ComparisonReport::derive tests ---

    #[test]
    fn verdict_canonical_match() {
        let sqlite3 = cr(Some(true), Some("raw1"), Some("canon"), Some("log"));
        let fsqlite = cr(Some(true), Some("raw2"), Some("canon"), Some("log"));
        let report = ComparisonReport::derive(&sqlite3, &fsqlite);
        assert!(matches!(report.verdict, ComparisonVerdict::Match));
        assert!(report.explanation.contains("canonical_sha256 match"));
    }

    #[test]
    fn verdict_canonical_mismatch() {
        let sqlite3 = cr(Some(true), None, Some("a"), Some("log"));
        let fsqlite = cr(Some(true), None, Some("b"), Some("log"));
        let report = ComparisonReport::derive(&sqlite3, &fsqlite);
        assert!(matches!(report.verdict, ComparisonVerdict::Mismatch));
        assert!(report.explanation.contains("canonical_sha256 MISMATCH"));
    }

    #[test]
    fn verdict_logical_fallback_match() {
        let sqlite3 = cr(Some(true), None, None, Some("log"));
        let fsqlite = cr(Some(true), None, None, Some("log"));
        let report = ComparisonReport::derive(&sqlite3, &fsqlite);
        assert!(matches!(report.verdict, ComparisonVerdict::Match));
        assert!(report.explanation.contains("logical match"));
    }

    #[test]
    fn verdict_logical_fallback_mismatch() {
        let sqlite3 = cr(Some(true), None, None, Some("log1"));
        let fsqlite = cr(Some(true), None, None, Some("log2"));
        let report = ComparisonReport::derive(&sqlite3, &fsqlite);
        assert!(matches!(report.verdict, ComparisonVerdict::Mismatch));
        assert!(report.explanation.contains("logical MISMATCH"));
    }

    #[test]
    fn verdict_error_when_no_decisive_tier() {
        let sqlite3 = cr(None, None, None, None);
        let fsqlite = cr(None, None, None, None);
        let report = ComparisonReport::derive(&sqlite3, &fsqlite);
        assert!(matches!(report.verdict, ComparisonVerdict::Error));
        assert!(report.explanation.contains("insufficient data"));
    }

    #[test]
    fn verdict_error_includes_raw_info_when_available() {
        let sqlite3 = cr(None, Some("r"), None, None);
        let fsqlite = cr(None, Some("r"), None, None);
        let report = ComparisonReport::derive(&sqlite3, &fsqlite);
        assert!(matches!(report.verdict, ComparisonVerdict::Error));
        assert!(report.explanation.contains("raw_sha256 matches"));
    }

    #[test]
    fn verdict_canonical_takes_priority_over_logical() {
        // Canonical match but logical would mismatch — canonical wins.
        let sqlite3 = cr(Some(true), None, Some("c"), Some("l1"));
        let fsqlite = cr(Some(true), None, Some("c"), Some("l2"));
        let report = ComparisonReport::derive(&sqlite3, &fsqlite);
        assert!(matches!(report.verdict, ComparisonVerdict::Match));
        assert!(report.explanation.contains("canonical_sha256 match"));
    }
}
