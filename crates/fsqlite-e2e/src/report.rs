use serde::{Deserialize, Serialize};

/// JSON schema version for the E2E report format.
///
/// This is a human-readable version string intended for `report.json` consumers.
pub const REPORT_SCHEMA_V1: &str = "fsqlite-e2e.report.v1";

/// JSON schema version for per-run JSONL records.
///
/// Each JSONL line should contain exactly one [`RunRecordV1`] object.
pub const RUN_RECORD_SCHEMA_V1: &str = "fsqlite-e2e.run_record.v1";

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

/// A single JSONL record for a single-engine run.
///
/// This is intentionally a "flat" record suitable for append-only JSONL logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecordV1 {
    pub schema_version: String,
    /// Milliseconds since Unix epoch, captured when the record is written.
    pub recorded_unix_ms: u64,
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
        Self {
            schema_version: RUN_RECORD_SCHEMA_V1.to_owned(),
            recorded_unix_ms: args.recorded_unix_ms,
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
        };

        let record = RunRecordV1::new(RunRecordV1Args {
            recorded_unix_ms: 1_700_000_000_000,
            engine: EngineInfo {
                name: "sqlite3".to_owned(),
                sqlite_version: Some("3.45.1".to_owned()),
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
        assert_eq!(parsed.engine.name, "sqlite3");
        assert_eq!(parsed.concurrency, 4);
        assert_eq!(parsed.ops_count, 10);
        assert_eq!(parsed.report.wall_time_ms, 123);
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
