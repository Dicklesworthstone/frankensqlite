use serde::{Deserialize, Serialize};

/// JSON schema version for the E2E report format.
///
/// This is a human-readable version string intended for `report.json` consumers.
pub const REPORT_SCHEMA_V1: &str = "fsqlite-e2e.report.v1";

/// Top-level report for a single E2E run.
///
/// A run may contain multiple benchmark/correctness cases (fixture × workload × concurrency).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2eReport {
    pub schema_version: String,
    pub run: RunInfo,
    pub fixture: FixtureInfo,
    pub workload: WorkloadInfo,
    pub cases: Vec<CaseReport>,
}

impl E2eReport {
    pub fn new(run: RunInfo, fixture: FixtureInfo, workload: WorkloadInfo) -> Self {
        Self {
            schema_version: REPORT_SCHEMA_V1.to_owned(),
            run,
            fixture,
            workload,
            cases: Vec::new(),
        }
    }
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
    pub integrity_check_ok: Option<bool>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub verdict: ComparisonVerdict,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonVerdict {
    Match,
    Mismatch,
    Error,
}
