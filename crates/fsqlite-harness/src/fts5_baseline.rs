//! Baseline harness for stock SQLite FTS5 vs FrankenSQLite's current FTS5 path.
//!
//! This is the executable bd-2nzo8.1.3 artifact.  It intentionally records
//! today's materialized/in-memory deltas instead of treating them as parity
//! failures; later shadow-backed work can reuse the same scenarios and artifact
//! shape as a regression and performance target.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::differential_v2::NormalizedValue;

/// Bead that owns this baseline harness.
pub const BEAD_ID: &str = "bd-2nzo8.1.3";
/// Stable seed used unless the caller supplies `SEED`.
pub const DEFAULT_SEED: u64 = 2_020_408_013;
/// Default number of repeated read probes per benchmark label.
pub const DEFAULT_PERF_ITERATIONS: usize = 3;
const SCHEMA_VERSION: &str = "1.0.0";
const LOG_SCHEMA_VERSION: &str = "1.0.0";
const SQLITE_ORACLE_BACKEND: &str = "stock-sqlite-fts5";
const FSQLITE_BACKEND: &str = "frankensqlite-materialized";

/// Runtime configuration for a baseline run.
#[derive(Debug, Clone)]
pub struct Fts5BaselineConfig {
    /// Run identifier used in logs and artifact paths.
    pub run_id: String,
    /// Trace identifier used for cross-artifact correlation.
    pub trace_id: String,
    /// Deterministic seed for fixture selection.
    pub seed: u64,
    /// Root directory that receives `bd-2nzo8.1.3/<run_id>/`.
    pub artifact_root: PathBuf,
    /// Repetitions for cheap read-latency probes.
    pub perf_iterations: usize,
    /// Optional scenario filter for replay.
    pub scenario_id: Option<String>,
}

impl Fts5BaselineConfig {
    /// Build a config from the proof-contract replay environment.
    #[must_use]
    pub fn from_env() -> Self {
        let run_id = env::var("RUN_ID").unwrap_or_else(|_| default_run_id());
        let trace_id = env::var("TRACE_ID").unwrap_or_else(|_| format!("fts5-baseline-{run_id}"));
        let seed = env::var("SEED")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SEED);
        let perf_iterations = env::var("FSQLITE_FTS5_PERF_ITERATIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PERF_ITERATIONS);
        let artifact_root = env::var("FSQLITE_FTS5_ARTIFACT_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_artifact_root());
        let scenario_id = env::var("SCENARIO_ID")
            .ok()
            .filter(|value| !value.is_empty());

        Self {
            run_id,
            trace_id,
            seed,
            artifact_root,
            perf_iterations,
            scenario_id,
        }
    }

    /// Artifact bundle directory for this run.
    #[must_use]
    pub fn bundle_dir(&self) -> PathBuf {
        self.artifact_root.join(BEAD_ID).join(&self.run_id)
    }
}

/// Complete report returned by a baseline run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fts5BaselineReport {
    /// Report schema version.
    pub schema_version: String,
    /// Bead identifier.
    pub bead_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Trace identifier.
    pub trace_id: String,
    /// Deterministic seed.
    pub seed: u64,
    /// SQLite version reported by rusqlite's bundled oracle.
    pub oracle_sqlite_version: String,
    /// Harness result. Baseline divergences do not make the harness fail.
    pub result: String,
    /// Scenario summaries in deterministic order.
    pub scenarios: Vec<ScenarioReport>,
    /// Aggregated benchmark rows.
    pub benchmark_rows: Vec<BenchmarkRow>,
    /// Memory and IO summary for the process-level baseline run.
    pub memory_io: MemoryIoSummary,
    /// First divergence across all scenarios.
    pub first_divergence: Option<FirstDivergence>,
    /// Artifact bundle paths and hashes.
    pub artifacts: ArtifactBundleSummary,
}

impl Fts5BaselineReport {
    /// Count scenarios with at least one behavior or catalog divergence.
    #[must_use]
    pub fn divergent_scenario_count(&self) -> usize {
        self.scenarios
            .iter()
            .filter(|scenario| scenario.first_divergence.is_some())
            .count()
    }
}

/// Per-scenario baseline report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    /// Stable scenario identifier.
    pub scenario_id: String,
    /// Stable fixture identifier.
    pub fixture_id: String,
    /// Shape summary for the database under test.
    pub db_shape: String,
    /// FTS5 content mode.
    pub content_mode: String,
    /// Tokenizer family.
    pub tokenizer: String,
    /// Detail mode.
    pub detail_mode: String,
    /// Columnsize mode.
    pub columnsize_mode: String,
    /// Locale mode.
    pub locale_enabled: bool,
    /// Tokendata mode.
    pub tokendata_enabled: bool,
    /// Rootpage expectation being measured.
    pub rootpage_mode: String,
    /// Command surfaces exercised by this scenario.
    pub command_surface: Vec<String>,
    /// Backend-specific reports.
    pub backends: Vec<BackendReport>,
    /// First backend divergence for this scenario, if any.
    pub first_divergence: Option<FirstDivergence>,
}

/// Backend-specific result for one scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendReport {
    /// Backend identifier.
    pub backend: String,
    /// Whether the backend completed the scenario sequence.
    pub result: String,
    /// Database path or `:memory:`.
    pub database_path: String,
    /// Initial open latency.
    pub open_elapsed_us: u128,
    /// Reopen latency if this scenario requires reopen measurement.
    pub reopen_elapsed_us: Option<u128>,
    /// Process RSS near the end of the backend run.
    pub rss_bytes: Option<u64>,
    /// Statement results in execution order.
    pub statements: Vec<StatementReport>,
}

/// One measured SQL statement result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementReport {
    /// Stable label for aggregation.
    pub label: String,
    /// Scenario phase (`setup`, `execute`, `validate`, `teardown`).
    pub phase: String,
    /// One-based iteration number for repeated probes.
    pub iteration: usize,
    /// SQL text.
    pub sql: String,
    /// Normalized statement outcome.
    pub outcome: BaselineOutcome,
    /// Elapsed wall time in microseconds.
    pub elapsed_us: u128,
}

/// Normalized statement outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BaselineOutcome {
    /// Statement returned rows.
    Rows {
        /// Normalized rows.
        rows: Vec<Vec<NormalizedValue>>,
    },
    /// Statement executed without result rows.
    Execute {
        /// Affected row count reported by the backend.
        affected_rows: usize,
    },
    /// Statement failed. For baseline comparisons, two errors match by class.
    Error {
        /// Backend error string.
        message: String,
    },
}

/// First divergence between the two backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstDivergence {
    /// Scenario where the divergence occurred.
    pub scenario_id: String,
    /// Statement label.
    pub label: String,
    /// SQL text.
    pub sql: String,
    /// Stock SQLite outcome.
    pub stock_sqlite: BaselineOutcome,
    /// FrankenSQLite outcome.
    pub frankensqlite: BaselineOutcome,
}

/// Aggregated benchmark row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRow {
    /// Scenario identifier.
    pub scenario_id: String,
    /// Backend identifier.
    pub backend: String,
    /// Statement label.
    pub label: String,
    /// Number of measurements.
    pub iterations: usize,
    /// Minimum latency.
    pub min_us: u128,
    /// Mean latency.
    pub mean_us: u128,
    /// Maximum latency.
    pub max_us: u128,
}

/// Process-level memory and IO summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryIoSummary {
    /// RSS before running scenarios.
    pub rss_before_bytes: Option<u64>,
    /// RSS after running scenarios.
    pub rss_after_bytes: Option<u64>,
    /// Number of backend scenario executions.
    pub backend_runs: usize,
    /// Number of measured statements.
    pub measured_statements: usize,
}

/// Artifact path and hash summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactBundleSummary {
    /// Bundle root path.
    pub bundle_dir: String,
    /// Paths keyed by artifact role.
    pub paths: BTreeMap<String, String>,
    /// SHA-256 hashes keyed by artifact role.
    pub hashes: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct ScenarioSpec {
    scenario_id: &'static str,
    fixture_id: &'static str,
    db_shape: &'static str,
    content_mode: &'static str,
    tokenizer: &'static str,
    detail_mode: &'static str,
    columnsize_mode: &'static str,
    rootpage_mode: &'static str,
    command_surface: Vec<&'static str>,
    flags: ScenarioFlags,
    setup: Vec<SqlStep>,
    workload: Vec<SqlStep>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ScenarioFlags(u8);

impl ScenarioFlags {
    const FILE_BACKED: u8 = 0b0001;
    const LOCALE_ENABLED: u8 = 0b0010;
    const REOPEN_BEFORE_WORKLOAD: u8 = 0b0100;
    const TOKENDATA_ENABLED: u8 = 0b1000;

    const fn empty() -> Self {
        Self(0)
    }

    const fn file_backed(self) -> Self {
        Self(self.0 | Self::FILE_BACKED)
    }

    const fn reopen_before_workload(self) -> Self {
        Self(self.0 | Self::REOPEN_BEFORE_WORKLOAD)
    }

    const fn has(self, flag: u8) -> bool {
        self.0 & flag != 0
    }

    const fn is_file_backed(self) -> bool {
        self.has(Self::FILE_BACKED)
    }

    const fn is_locale_enabled(self) -> bool {
        self.has(Self::LOCALE_ENABLED)
    }

    const fn is_reopen_before_workload(self) -> bool {
        self.has(Self::REOPEN_BEFORE_WORKLOAD)
    }

    const fn is_tokendata_enabled(self) -> bool {
        self.has(Self::TOKENDATA_ENABLED)
    }
}

#[derive(Debug, Clone, Copy)]
struct SqlStep {
    label: &'static str,
    phase: &'static str,
    sql: &'static str,
    repeats: StepRepeats,
}

#[derive(Debug, Clone, Copy)]
enum StepRepeats {
    Once,
    PerfIterations,
}

#[derive(Debug, Clone, Copy)]
enum BackendKind {
    FrankenSqlite,
    StockSqlite,
}

impl BackendKind {
    fn label(self) -> &'static str {
        match self {
            Self::FrankenSqlite => FSQLITE_BACKEND,
            Self::StockSqlite => SQLITE_ORACLE_BACKEND,
        }
    }
}

enum Engine {
    FrankenSqlite(Box<fsqlite::Connection>),
    StockSqlite(Box<rusqlite::Connection>),
}

impl Engine {
    fn open(kind: BackendKind, path: Option<&Path>) -> Result<Self, String> {
        match kind {
            BackendKind::FrankenSqlite => {
                let target = path
                    .map(path_to_string)
                    .unwrap_or_else(|| ":memory:".to_owned());
                crate::block_on(fsqlite::Connection::open(target))
                    .map(Box::new)
                    .map(Self::FrankenSqlite)
                    .map_err(|error| error.to_string())
            }
            BackendKind::StockSqlite => match path {
                Some(path) => rusqlite::Connection::open(path)
                    .map(Box::new)
                    .map(Self::StockSqlite)
                    .map_err(|error| error.to_string()),
                None => rusqlite::Connection::open_in_memory()
                    .map(Box::new)
                    .map(Self::StockSqlite)
                    .map_err(|error| error.to_string()),
            },
        }
    }

    fn run(&self, sql: &str) -> BaselineOutcome {
        if stmt_returns_rows(sql) {
            match self.query(sql) {
                Ok(rows) => BaselineOutcome::Rows { rows },
                Err(error) => BaselineOutcome::Error { message: error },
            }
        } else {
            match self.execute(sql) {
                Ok(affected_rows) => BaselineOutcome::Execute { affected_rows },
                Err(error) => BaselineOutcome::Error { message: error },
            }
        }
    }

    fn execute(&self, sql: &str) -> Result<usize, String> {
        match self {
            Self::FrankenSqlite(conn) => {
                crate::block_on(conn.execute(sql.trim())).map_err(|error| error.to_string())
            }
            Self::StockSqlite(conn) => conn
                .execute(sql.trim(), [])
                .map_err(|error| error.to_string()),
        }
    }

    fn query(&self, sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String> {
        match self {
            Self::FrankenSqlite(conn) => crate::block_on(conn.query(sql.trim()))
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| {
                            row.values()
                                .iter()
                                .map(fsqlite_value_to_normalized)
                                .collect()
                        })
                        .collect()
                })
                .map_err(|error| error.to_string()),
            Self::StockSqlite(conn) => query_rusqlite(conn, sql),
        }
    }
}

/// Canonical S1.3 scenarios.
#[must_use]
pub fn canonical_scenarios() -> Vec<String> {
    scenario_specs()
        .iter()
        .map(|scenario| scenario.scenario_id.to_owned())
        .collect()
}

/// Run the full baseline suite and write the proof-artifact bundle.
///
/// # Errors
///
/// Returns a string when setup, execution, or artifact writing fails.
pub fn run_all_baselines(config: &Fts5BaselineConfig) -> Result<Fts5BaselineReport, String> {
    let bundle_dir = config.bundle_dir();
    fs::create_dir_all(&bundle_dir).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=bundle_dir_create_failed path={} error={error}",
            bundle_dir.display()
        )
    })?;

    let mut events = Vec::new();
    events.push(event(
        config,
        "setup",
        "start",
        None,
        json!({
            "bead_id": BEAD_ID,
            "artifact_paths": bundle_dir.display().to_string(),
            "invariant_ids": ["FTS5-S1.3-ARTIFACT-BUNDLE", "FTS5-S1.3-BASELINE"],
        }),
    ));

    let rss_before_bytes = current_rss_bytes();
    let mut scenarios = Vec::new();

    for scenario in scenario_specs()
        .into_iter()
        .filter(|scenario| scenario_matches_filter(scenario, config.scenario_id.as_deref()))
    {
        events.push(event(
            config,
            "setup",
            "info",
            Some(scenario.scenario_id),
            scenario_context(&scenario, json!({"backend": "both"})),
        ));
        scenarios.push(run_scenario(config, &bundle_dir, scenario)?);
    }

    let benchmark_rows = build_benchmark_rows(&scenarios);
    let memory_io = MemoryIoSummary {
        rss_before_bytes,
        rss_after_bytes: current_rss_bytes(),
        backend_runs: scenarios
            .iter()
            .map(|scenario| scenario.backends.len())
            .sum::<usize>(),
        measured_statements: scenarios
            .iter()
            .flat_map(|scenario| &scenario.backends)
            .map(|backend| backend.statements.len())
            .sum(),
    };
    let first_divergence = scenarios
        .iter()
        .find_map(|scenario| scenario.first_divergence.clone());

    for scenario in &scenarios {
        let event_type = if scenario.first_divergence.is_some() {
            "first_divergence"
        } else {
            "info"
        };
        events.push(event(
            config,
            "validate",
            event_type,
            Some(&scenario.scenario_id),
            json!({
                "bead_id": BEAD_ID,
                "fixture_id": scenario.fixture_id,
                "diff_oracle": SQLITE_ORACLE_BACKEND,
                "first_divergence": scenario.first_divergence,
            }),
        ));
    }

    events.push(event(
        config,
        "execute",
        "info",
        None,
        json!({
            "bead_id": BEAD_ID,
            "query_count": memory_io.measured_statements,
            "rss_bytes": memory_io.rss_after_bytes,
        }),
    ));

    let mut report = Fts5BaselineReport {
        schema_version: SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        run_id: config.run_id.clone(),
        trace_id: config.trace_id.clone(),
        seed: config.seed,
        oracle_sqlite_version: rusqlite::version().to_owned(),
        result: "pass".to_owned(),
        scenarios,
        benchmark_rows,
        memory_io,
        first_divergence,
        artifacts: ArtifactBundleSummary {
            bundle_dir: bundle_dir.display().to_string(),
            paths: BTreeMap::new(),
            hashes: BTreeMap::new(),
        },
    };

    write_artifact_bundle(config, &bundle_dir, &mut events, &mut report)?;
    Ok(report)
}

fn scenario_specs() -> Vec<ScenarioSpec> {
    vec![
        ScenarioSpec {
            scenario_id: "FTS5-S1-STORED-CREATE-MATCH-001",
            fixture_id: "stored_porter_small",
            db_shape: "stored+porter+small",
            content_mode: "stored",
            tokenizer: "porter",
            detail_mode: "full",
            columnsize_mode: "table",
            rootpage_mode: "stock_zero_vs_current_materialized",
            command_surface: vec!["match", "highlight", "schema-rootpage"],
            flags: ScenarioFlags::empty(),
            setup: vec![
                step(
                    "create_docs",
                    "setup",
                    "CREATE VIRTUAL TABLE docs USING fts5(title, body, tokenize='porter')",
                ),
                step(
                    "insert_docs",
                    "setup",
                    "INSERT INTO docs(rowid, title, body) VALUES (1, 'Rust Search', 'Rust powers fast search')",
                ),
                step(
                    "insert_docs",
                    "setup",
                    "INSERT INTO docs(rowid, title, body) VALUES (2, 'SQLite Notes', 'stock sqlite keeps shadow tables')",
                ),
                step(
                    "insert_docs",
                    "setup",
                    "INSERT INTO docs(rowid, title, body) VALUES (3, 'Concurrent Writers', 'mvcc keeps writers independent')",
                ),
            ],
            workload: vec![
                perf_step(
                    "match_latency",
                    "execute",
                    "SELECT rowid FROM docs WHERE docs MATCH 'rust' ORDER BY rowid",
                ),
                step(
                    "highlight_aux",
                    "validate",
                    "SELECT highlight(docs, 1, '[', ']') FROM docs WHERE docs MATCH 'rust' ORDER BY rowid",
                ),
                step(
                    "rootpage_contract",
                    "validate",
                    "SELECT rootpage FROM sqlite_master WHERE name = 'docs'",
                ),
                step(
                    "shadow_table_catalog",
                    "validate",
                    "SELECT name FROM sqlite_master WHERE name LIKE 'docs_%' ORDER BY name",
                ),
            ],
        },
        ScenarioSpec {
            scenario_id: "FTS5-S1-REOPEN-SHADOW-LAYOUT-002",
            fixture_id: "file_backed_reopen_shadow_layout",
            db_shape: "file_backed+stored+reopen",
            content_mode: "stored",
            tokenizer: "unicode61",
            detail_mode: "full",
            columnsize_mode: "table",
            rootpage_mode: "stock_zero_vs_current_materialized",
            command_surface: vec!["open", "reopen", "match", "schema-shadow-table-list"],
            flags: ScenarioFlags::empty()
                .file_backed()
                .reopen_before_workload(),
            setup: vec![
                step(
                    "create_docs",
                    "setup",
                    "CREATE VIRTUAL TABLE docs USING fts5(title, body)",
                ),
                step(
                    "insert_docs",
                    "setup",
                    "INSERT INTO docs(rowid, title, body) VALUES (1, 'Alpha', 'alpha beta gamma')",
                ),
                step(
                    "insert_docs",
                    "setup",
                    "INSERT INTO docs(rowid, title, body) VALUES (2, 'Beta', 'beta gamma delta')",
                ),
            ],
            workload: vec![
                perf_step(
                    "reopen_match_latency",
                    "execute",
                    "SELECT rowid FROM docs WHERE docs MATCH 'beta' ORDER BY rowid",
                ),
                step("reopen_count", "validate", "SELECT COUNT(*) FROM docs"),
                step(
                    "rootpage_contract",
                    "validate",
                    "SELECT rootpage FROM sqlite_master WHERE name = 'docs'",
                ),
                step(
                    "shadow_table_catalog",
                    "validate",
                    "SELECT name FROM sqlite_master WHERE name LIKE 'docs_%' ORDER BY name",
                ),
            ],
        },
        ScenarioSpec {
            scenario_id: "FTS5-S1-CONTENTLESS-COMMAND-003",
            fixture_id: "contentless_optimize_small",
            db_shape: "contentless+porter+command-channel",
            content_mode: "contentless",
            tokenizer: "porter",
            detail_mode: "full",
            columnsize_mode: "table",
            rootpage_mode: "stock_zero_vs_current_materialized",
            command_surface: vec!["match", "optimize", "count"],
            flags: ScenarioFlags::empty(),
            setup: vec![
                step(
                    "create_docs",
                    "setup",
                    "CREATE VIRTUAL TABLE docs USING fts5(body, content='', tokenize='porter')",
                ),
                step(
                    "insert_docs",
                    "setup",
                    "INSERT INTO docs(rowid, body) VALUES (7, 'cats running with rust')",
                ),
            ],
            workload: vec![
                perf_step(
                    "contentless_match_latency",
                    "execute",
                    "SELECT rowid FROM docs WHERE docs MATCH 'cat' ORDER BY rowid",
                ),
                step(
                    "optimize_command",
                    "execute",
                    "INSERT INTO docs(docs) VALUES('optimize')",
                ),
                step("contentless_count", "validate", "SELECT COUNT(*) FROM docs"),
                step(
                    "rootpage_contract",
                    "validate",
                    "SELECT rootpage FROM sqlite_master WHERE name = 'docs'",
                ),
            ],
        },
        ScenarioSpec {
            scenario_id: "FTS5-S1-EXTERNAL-CONTENT-004",
            fixture_id: "external_content_rebuild_small",
            db_shape: "external_content+content_rowid+rebuild",
            content_mode: "external-content",
            tokenizer: "unicode61",
            detail_mode: "full",
            columnsize_mode: "table",
            rootpage_mode: "stock_zero_vs_current_materialized",
            command_surface: vec!["rebuild", "match", "schema-shadow-table-list"],
            flags: ScenarioFlags::empty(),
            setup: vec![
                step(
                    "create_content_table",
                    "setup",
                    "CREATE TABLE source_docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
                ),
                step(
                    "insert_content_table",
                    "setup",
                    "INSERT INTO source_docs(id, title, body) VALUES (1, 'One', 'external alpha body')",
                ),
                step(
                    "insert_content_table",
                    "setup",
                    "INSERT INTO source_docs(id, title, body) VALUES (2, 'Two', 'external beta body')",
                ),
                step(
                    "create_external_fts",
                    "setup",
                    "CREATE VIRTUAL TABLE docs_fts USING fts5(title, body, content='source_docs', content_rowid='id')",
                ),
            ],
            workload: vec![
                step(
                    "rebuild_command",
                    "execute",
                    "INSERT INTO docs_fts(docs_fts) VALUES('rebuild')",
                ),
                perf_step(
                    "external_match_latency",
                    "execute",
                    "SELECT rowid FROM docs_fts WHERE docs_fts MATCH 'alpha' ORDER BY rowid",
                ),
                step(
                    "rootpage_contract",
                    "validate",
                    "SELECT rootpage FROM sqlite_master WHERE name = 'docs_fts'",
                ),
                step(
                    "shadow_table_catalog",
                    "validate",
                    "SELECT name FROM sqlite_master WHERE name LIKE 'docs_fts_%' ORDER BY name",
                ),
            ],
        },
        ScenarioSpec {
            scenario_id: "FTS5-S1-DML-MAINTENANCE-005",
            fixture_id: "stored_dml_maintenance_small",
            db_shape: "stored+dml+maintenance",
            content_mode: "stored",
            tokenizer: "unicode61",
            detail_mode: "full",
            columnsize_mode: "table",
            rootpage_mode: "stock_zero_vs_current_materialized",
            command_surface: vec![
                "insert",
                "update",
                "delete",
                "rebuild",
                "integrity-check",
                "optimize",
            ],
            flags: ScenarioFlags::empty(),
            setup: vec![
                step(
                    "create_docs",
                    "setup",
                    "CREATE VIRTUAL TABLE docs USING fts5(body)",
                ),
                step(
                    "insert_docs",
                    "setup",
                    "INSERT INTO docs(rowid, body) VALUES (1, 'alpha beta')",
                ),
                step(
                    "insert_docs",
                    "setup",
                    "INSERT INTO docs(rowid, body) VALUES (2, 'beta gamma')",
                ),
            ],
            workload: vec![
                step(
                    "update_throughput_probe",
                    "execute",
                    "UPDATE docs SET body = 'alpha beta delta' WHERE rowid = 1",
                ),
                step(
                    "delete_throughput_probe",
                    "execute",
                    "DELETE FROM docs WHERE rowid = 2",
                ),
                step(
                    "insert_throughput_probe",
                    "execute",
                    "INSERT INTO docs(rowid, body) VALUES (3, 'gamma delta epsilon')",
                ),
                step(
                    "integrity_command",
                    "execute",
                    "INSERT INTO docs(docs) VALUES('integrity-check')",
                ),
                step(
                    "optimize_command",
                    "execute",
                    "INSERT INTO docs(docs) VALUES('optimize')",
                ),
                perf_step(
                    "post_dml_match_latency",
                    "execute",
                    "SELECT rowid FROM docs WHERE docs MATCH 'delta' ORDER BY rowid",
                ),
            ],
        },
    ]
}

const fn step(label: &'static str, phase: &'static str, sql: &'static str) -> SqlStep {
    SqlStep {
        label,
        phase,
        sql,
        repeats: StepRepeats::Once,
    }
}

const fn perf_step(label: &'static str, phase: &'static str, sql: &'static str) -> SqlStep {
    SqlStep {
        label,
        phase,
        sql,
        repeats: StepRepeats::PerfIterations,
    }
}

fn run_scenario(
    config: &Fts5BaselineConfig,
    bundle_dir: &Path,
    scenario: ScenarioSpec,
) -> Result<ScenarioReport, String> {
    let backends = vec![
        run_backend(config, bundle_dir, &scenario, BackendKind::StockSqlite)?,
        run_backend(config, bundle_dir, &scenario, BackendKind::FrankenSqlite)?,
    ];
    let first_divergence = find_first_divergence(&scenario, &backends);

    Ok(ScenarioReport {
        scenario_id: scenario.scenario_id.to_owned(),
        fixture_id: scenario.fixture_id.to_owned(),
        db_shape: scenario.db_shape.to_owned(),
        content_mode: scenario.content_mode.to_owned(),
        tokenizer: scenario.tokenizer.to_owned(),
        detail_mode: scenario.detail_mode.to_owned(),
        columnsize_mode: scenario.columnsize_mode.to_owned(),
        locale_enabled: scenario.flags.is_locale_enabled(),
        tokendata_enabled: scenario.flags.is_tokendata_enabled(),
        rootpage_mode: scenario.rootpage_mode.to_owned(),
        command_surface: scenario
            .command_surface
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        backends,
        first_divergence,
    })
}

fn run_backend(
    config: &Fts5BaselineConfig,
    bundle_dir: &Path,
    scenario: &ScenarioSpec,
    backend: BackendKind,
) -> Result<BackendReport, String> {
    let database_path = scenario
        .flags
        .is_file_backed()
        .then(|| backend_db_path(bundle_dir, scenario, backend));
    if let Some(parent) = database_path.as_ref().and_then(|path| path.parent()) {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=backend_work_dir_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }

    let open_start = Instant::now();
    let mut engine = Engine::open(backend, database_path.as_deref());
    let open_elapsed_us = open_start.elapsed().as_micros();
    let mut statements = Vec::new();

    if let Err(message) = &engine {
        return Ok(open_error_report(
            backend,
            database_path.as_ref(),
            open_elapsed_us,
            message.clone(),
        ));
    }

    let setup_ok = run_steps(
        engine.as_ref().expect("engine was checked above"),
        &scenario.setup,
        config.perf_iterations,
        &mut statements,
    );
    let mut reopen_elapsed_us = None;

    if setup_ok && scenario.flags.is_reopen_before_workload() {
        drop(engine);
        let reopen_start = Instant::now();
        let reopened = Engine::open(backend, database_path.as_deref());
        reopen_elapsed_us = Some(reopen_start.elapsed().as_micros());
        engine = reopened;
        if let Err(error) = engine.as_ref() {
            statements.push(StatementReport {
                label: "reopen".to_owned(),
                phase: "execute".to_owned(),
                iteration: 1,
                sql: String::new(),
                outcome: BaselineOutcome::Error {
                    message: error.clone(),
                },
                elapsed_us: reopen_elapsed_us.unwrap_or(0),
            });
        }
    }

    let workload_ok = if setup_ok {
        match engine.as_ref() {
            Ok(engine) => run_steps(
                engine,
                &scenario.workload,
                config.perf_iterations,
                &mut statements,
            ),
            Err(_) => false,
        }
    } else {
        false
    };

    let result = if setup_ok && workload_ok {
        "complete"
    } else {
        "captured_error"
    };

    Ok(BackendReport {
        backend: backend.label().to_owned(),
        result: result.to_owned(),
        database_path: database_path
            .as_ref()
            .map_or_else(|| ":memory:".to_owned(), |path| path_to_string(path)),
        open_elapsed_us,
        reopen_elapsed_us,
        rss_bytes: current_rss_bytes(),
        statements,
    })
}

fn open_error_report(
    backend: BackendKind,
    database_path: Option<&PathBuf>,
    open_elapsed_us: u128,
    message: String,
) -> BackendReport {
    BackendReport {
        backend: backend.label().to_owned(),
        result: "open_error".to_owned(),
        database_path: database_path
            .map_or_else(|| ":memory:".to_owned(), |path| path_to_string(path)),
        open_elapsed_us,
        reopen_elapsed_us: None,
        rss_bytes: current_rss_bytes(),
        statements: vec![StatementReport {
            label: "open".to_owned(),
            phase: "setup".to_owned(),
            iteration: 1,
            sql: String::new(),
            outcome: BaselineOutcome::Error { message },
            elapsed_us: open_elapsed_us,
        }],
    }
}

fn run_steps(
    engine: &Engine,
    steps: &[SqlStep],
    perf_iterations: usize,
    statements: &mut Vec<StatementReport>,
) -> bool {
    let mut ok = true;
    for step in steps {
        let repeats = match step.repeats {
            StepRepeats::Once => 1,
            StepRepeats::PerfIterations => perf_iterations,
        };
        for iteration in 1..=repeats {
            let start = Instant::now();
            let outcome = engine.run(step.sql);
            let elapsed_us = start.elapsed().as_micros();
            let is_error = matches!(outcome, BaselineOutcome::Error { .. });
            statements.push(StatementReport {
                label: step.label.to_owned(),
                phase: step.phase.to_owned(),
                iteration,
                sql: step.sql.to_owned(),
                outcome,
                elapsed_us,
            });
            if is_error {
                ok = false;
                break;
            }
        }
        if !ok {
            break;
        }
    }
    ok
}

fn find_first_divergence(
    scenario: &ScenarioSpec,
    backends: &[BackendReport],
) -> Option<FirstDivergence> {
    let stock = backends
        .iter()
        .find(|backend| backend.backend == SQLITE_ORACLE_BACKEND)?;
    let fsqlite = backends
        .iter()
        .find(|backend| backend.backend == FSQLITE_BACKEND)?;

    let max_len = stock.statements.len().max(fsqlite.statements.len());
    for index in 0..max_len {
        let stock_statement = stock.statements.get(index);
        let fsqlite_statement = fsqlite.statements.get(index);
        match (stock_statement, fsqlite_statement) {
            (Some(stock_statement), Some(fsqlite_statement))
                if outcomes_match(&stock_statement.outcome, &fsqlite_statement.outcome) => {}
            (Some(stock_statement), Some(fsqlite_statement)) => {
                return Some(FirstDivergence {
                    scenario_id: scenario.scenario_id.to_owned(),
                    label: stock_statement.label.clone(),
                    sql: stock_statement.sql.clone(),
                    stock_sqlite: stock_statement.outcome.clone(),
                    frankensqlite: fsqlite_statement.outcome.clone(),
                });
            }
            (Some(stock_statement), None) => {
                return Some(FirstDivergence {
                    scenario_id: scenario.scenario_id.to_owned(),
                    label: stock_statement.label.clone(),
                    sql: stock_statement.sql.clone(),
                    stock_sqlite: stock_statement.outcome.clone(),
                    frankensqlite: BaselineOutcome::Error {
                        message: "frankensqlite did not reach this statement".to_owned(),
                    },
                });
            }
            (None, Some(fsqlite_statement)) => {
                return Some(FirstDivergence {
                    scenario_id: scenario.scenario_id.to_owned(),
                    label: fsqlite_statement.label.clone(),
                    sql: fsqlite_statement.sql.clone(),
                    stock_sqlite: BaselineOutcome::Error {
                        message: "stock sqlite did not reach this statement".to_owned(),
                    },
                    frankensqlite: fsqlite_statement.outcome.clone(),
                });
            }
            (None, None) => {}
        }
    }
    None
}

fn outcomes_match(left: &BaselineOutcome, right: &BaselineOutcome) -> bool {
    match (left, right) {
        (BaselineOutcome::Rows { rows: left }, BaselineOutcome::Rows { rows: right }) => {
            left == right
        }
        (
            BaselineOutcome::Execute {
                affected_rows: left,
            },
            BaselineOutcome::Execute {
                affected_rows: right,
            },
        ) => left == right,
        (BaselineOutcome::Error { .. }, BaselineOutcome::Error { .. }) => true,
        _ => false,
    }
}

fn build_benchmark_rows(scenarios: &[ScenarioReport]) -> Vec<BenchmarkRow> {
    let mut buckets: BTreeMap<(String, String, String), Vec<u128>> = BTreeMap::new();

    for scenario in scenarios {
        for backend in &scenario.backends {
            for statement in &backend.statements {
                if matches!(statement.outcome, BaselineOutcome::Error { .. }) {
                    continue;
                }
                buckets
                    .entry((
                        scenario.scenario_id.clone(),
                        backend.backend.clone(),
                        statement.label.clone(),
                    ))
                    .or_default()
                    .push(statement.elapsed_us);
            }
        }
    }

    buckets
        .into_iter()
        .filter_map(|((scenario_id, backend, label), samples)| {
            let iterations = samples.len();
            if iterations == 0 {
                return None;
            }
            let min_us = *samples.iter().min()?;
            let max_us = *samples.iter().max()?;
            let total_us = samples.iter().sum::<u128>();
            Some(BenchmarkRow {
                scenario_id,
                backend,
                label,
                iterations,
                min_us,
                mean_us: total_us / iterations as u128,
                max_us,
            })
        })
        .collect()
}

fn write_artifact_bundle(
    config: &Fts5BaselineConfig,
    bundle_dir: &Path,
    events: &mut Vec<EventRecord>,
    report: &mut Fts5BaselineReport,
) -> Result<(), String> {
    let summary_path = bundle_dir.join("summary.json");
    let diff_path = bundle_dir.join("diff_report.json");
    let benchmark_path = bundle_dir.join("benchmark_summary.json");
    let memory_path = bundle_dir.join("memory_io_summary.json");
    let replay_path = bundle_dir.join("replay.env");
    let manifest_path = bundle_dir.join("manifest.json");
    let hashes_path = bundle_dir.join("artifact_hashes.txt");
    let events_path = bundle_dir.join("events.jsonl");

    write_json_file(
        &diff_path,
        &json!({
            "schema_version": SCHEMA_VERSION,
            "bead_id": BEAD_ID,
            "run_id": &config.run_id,
            "first_divergence": &report.first_divergence,
            "scenario_divergences": report
                .scenarios
                .iter()
                .filter_map(|scenario| scenario.first_divergence.as_ref())
                .collect::<Vec<_>>(),
        }),
    )?;
    write_json_file(&benchmark_path, &report.benchmark_rows)?;
    write_json_file(&memory_path, &report.memory_io)?;
    fs::write(&replay_path, replay_env(config)).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=replay_env_write_failed path={} error={error}",
            replay_path.display()
        )
    })?;

    let mut artifact_paths = BTreeMap::new();
    artifact_paths.insert("diff_report_json".to_owned(), path_to_string(&diff_path));
    artifact_paths.insert(
        "benchmark_summary_json".to_owned(),
        path_to_string(&benchmark_path),
    );
    artifact_paths.insert(
        "memory_io_summary_json".to_owned(),
        path_to_string(&memory_path),
    );
    artifact_paths.insert("replay_env".to_owned(), path_to_string(&replay_path));

    let mut artifact_hashes = BTreeMap::new();
    for (name, path) in &artifact_paths {
        artifact_hashes.insert(name.clone(), sha256_file(Path::new(path))?);
    }

    events.push(event(
        config,
        "report",
        "artifact_generated",
        None,
        json!({
            "bead_id": BEAD_ID,
            "artifact_paths": &artifact_paths,
            "artifact_hashes": &artifact_hashes,
            "result": &report.result,
            "elapsed_ms": 0,
        }),
    ));
    events.push(event(
        config,
        "teardown",
        "pass",
        None,
        json!({
            "bead_id": BEAD_ID,
            "result": &report.result,
            "divergent_scenario_count": report.divergent_scenario_count(),
        }),
    ));
    write_events(&events_path, events)?;
    artifact_paths.insert("events_jsonl".to_owned(), path_to_string(&events_path));
    artifact_hashes.insert("events_jsonl".to_owned(), sha256_file(&events_path)?);
    artifact_paths.insert("summary_json".to_owned(), path_to_string(&summary_path));
    artifact_paths.insert(
        "artifact_hashes_txt".to_owned(),
        path_to_string(&hashes_path),
    );

    let manifest = build_manifest(config, report, &artifact_paths, &artifact_hashes);
    write_json_file(&manifest_path, &manifest)?;
    artifact_paths.insert("manifest_json".to_owned(), path_to_string(&manifest_path));
    artifact_hashes.insert("manifest_json".to_owned(), sha256_file(&manifest_path)?);

    report.artifacts = ArtifactBundleSummary {
        bundle_dir: bundle_dir.display().to_string(),
        paths: artifact_paths.clone(),
        hashes: artifact_hashes.clone(),
    };
    write_json_file(&summary_path, report)?;
    artifact_hashes.insert("summary_json".to_owned(), sha256_file(&summary_path)?);

    write_hashes(&hashes_path, &artifact_hashes)?;
    artifact_hashes.insert("artifact_hashes_txt".to_owned(), sha256_file(&hashes_path)?);

    report.artifacts = ArtifactBundleSummary {
        bundle_dir: bundle_dir.display().to_string(),
        paths: artifact_paths,
        hashes: artifact_hashes,
    };
    Ok(())
}

fn build_manifest(
    config: &Fts5BaselineConfig,
    report: &Fts5BaselineReport,
    artifact_paths: &BTreeMap<String, String>,
    artifact_hashes: &BTreeMap<String, String>,
) -> Value {
    let scenario_ids = report
        .scenarios
        .iter()
        .map(|scenario| scenario.scenario_id.clone())
        .collect::<Vec<_>>();
    json!({
        "schema_version": SCHEMA_VERSION,
        "bead_id": BEAD_ID,
        "run_id": &config.run_id,
        "trace_id": &config.trace_id,
        "scenario_id": scenario_ids.join(","),
        "seed": config.seed,
        "backend": FSQLITE_BACKEND,
        "oracle_backend": SQLITE_ORACLE_BACKEND,
        "fixture_id": "fts5_s1_baseline_corpus",
        "fixture_fingerprint": fixture_fingerprint(report),
        "db_shape": "stored+contentless+external_content+file_reopen",
        "content_mode": "stored/contentless/external-content",
        "tokenizer": "unicode61/porter",
        "detail_mode": "full",
        "columnsize_mode": "table",
        "locale_enabled": false,
        "tokendata_enabled": false,
        "rootpage_mode": "stock_zero_vs_current_materialized",
        "shadow_table_layout": "stock_fts5_v1_vs_current_materialized",
        "command_surface": ["match", "highlight", "rebuild", "integrity-check", "optimize", "dml"],
        "mvcc_mode": "begin_concurrent_default_preserved",
        "migration_mode": "baseline_only",
        "replay_command": replay_command(config),
        "artifact_paths": artifact_paths,
        "artifact_hashes": artifact_hashes,
        "result": &report.result,
        "started_at": &config.run_id,
        "finished_at": now_timestamp(),
    })
}

fn fixture_fingerprint(report: &Fts5BaselineReport) -> String {
    let mut hasher = Sha256::new();
    for scenario in &report.scenarios {
        hasher.update(scenario.scenario_id.as_bytes());
        hasher.update(scenario.fixture_id.as_bytes());
        hasher.update(scenario.db_shape.as_bytes());
    }
    format!("sha256:{}", crate::bytes_to_lower_hex(hasher.finalize()))
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=json_serialize_failed path={} error={error}",
            path.display()
        )
    })?;
    fs::write(path, bytes).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=json_write_failed path={} error={error}",
            path.display()
        )
    })
}

fn write_events(path: &Path, events: &[EventRecord]) -> Result<(), String> {
    let mut out = String::new();
    for event in events {
        let line = serde_json::to_string(event).map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=event_serialize_failed path={} error={error}",
                path.display()
            )
        })?;
        out.push_str(&line);
        out.push('\n');
    }
    fs::write(path, out).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=events_write_failed path={} error={error}",
            path.display()
        )
    })
}

fn write_hashes(path: &Path, hashes: &BTreeMap<String, String>) -> Result<(), String> {
    let mut out = String::new();
    for (name, hash) in hashes {
        out.push_str(hash);
        out.push(' ');
        out.push_str(name);
        out.push('\n');
    }
    fs::write(path, out).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=hashes_write_failed path={} error={error}",
            path.display()
        )
    })
}

#[derive(Debug, Clone, Serialize)]
struct EventRecord {
    run_id: String,
    timestamp: String,
    phase: String,
    event_type: String,
    scenario_id: Option<String>,
    seed: u64,
    backend: String,
    context: Value,
    log_schema_version: String,
}

fn event(
    config: &Fts5BaselineConfig,
    phase: &str,
    event_type: &str,
    scenario_id: Option<&str>,
    context: Value,
) -> EventRecord {
    EventRecord {
        run_id: config.run_id.clone(),
        timestamp: now_timestamp(),
        phase: phase.to_owned(),
        event_type: event_type.to_owned(),
        scenario_id: scenario_id.map(str::to_owned),
        seed: config.seed,
        backend: "both".to_owned(),
        context,
        log_schema_version: LOG_SCHEMA_VERSION.to_owned(),
    }
}

fn scenario_context(scenario: &ScenarioSpec, extra: Value) -> Value {
    json!({
        "bead_id": BEAD_ID,
        "fixture_id": scenario.fixture_id,
        "db_shape": scenario.db_shape,
        "content_mode": scenario.content_mode,
        "content_rowid_mode": if scenario.content_mode == "external-content" { "content_rowid=id" } else { "rowid" },
        "tokenizer": scenario.tokenizer,
        "prefix_config": "none",
        "detail_mode": scenario.detail_mode,
        "columnsize_mode": scenario.columnsize_mode,
        "locale_enabled": scenario.flags.is_locale_enabled(),
        "tokendata_enabled": scenario.flags.is_tokendata_enabled(),
        "command_name": scenario.command_surface.join(","),
        "command_args": "canonical-small-corpus",
        "invariant_ids": ["FTS5-S1.3-STOCK-ORACLE", "FTS5-S1.3-MATERIALIZED-BASELINE"],
        "artifact_paths": "",
        "diff_oracle": SQLITE_ORACLE_BACKEND,
        "rootpage_mode": scenario.rootpage_mode,
        "shadow_table_names": "docs_config,docs_content,docs_docsize,docs_data,docs_idx",
        "segment_generation": "baseline-current",
        "structure_record_version": "stock-v1-or-v2-vs-current-none",
        "page_conflicts": 0,
        "busy_class": "none",
        "migration_mode": "baseline_only",
        "downstream_repo": Value::Null,
        "extra": extra,
    })
}

fn replay_env(config: &Fts5BaselineConfig) -> String {
    format!(
        "export RUN_ID='{}'\nexport TRACE_ID='{}'\nexport SCENARIO_ID='{}'\nexport SEED='{}'\nexport FSQLITE_FTS5_ARTIFACT_ROOT='{}'\nexport FSQLITE_FTS5_PERF_ITERATIONS='{}'\n",
        config.run_id,
        config.trace_id,
        config.scenario_id.clone().unwrap_or_default(),
        config.seed,
        config.artifact_root.display(),
        config.perf_iterations,
    )
}

fn replay_command(config: &Fts5BaselineConfig) -> String {
    format!(
        "RUN_ID={} TRACE_ID={} SEED={} FSQLITE_FTS5_ARTIFACT_ROOT={} FSQLITE_FTS5_PERF_ITERATIONS={} ./scripts/verify_bd_2nzo8_1_3_fts5_baseline.sh --json",
        config.run_id,
        config.trace_id,
        config.seed,
        config.artifact_root.display(),
        config.perf_iterations,
    )
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=hash_read_failed path={} error={error}",
            path.display()
        )
    })?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!(
        "sha256:{}",
        crate::bytes_to_lower_hex(hasher.finalize())
    ))
}

fn query_rusqlite(
    conn: &rusqlite::Connection,
    sql: &str,
) -> Result<Vec<Vec<NormalizedValue>>, String> {
    let mut stmt = conn
        .prepare(sql.trim())
        .map_err(|error| error.to_string())?;
    let col_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(col_count);
            for index in 0..col_count {
                let value: rusqlite::types::Value =
                    row.get(index).unwrap_or(rusqlite::types::Value::Null);
                values.push(match value {
                    rusqlite::types::Value::Null => NormalizedValue::Null,
                    rusqlite::types::Value::Integer(value) => NormalizedValue::Integer(value),
                    rusqlite::types::Value::Real(value) => NormalizedValue::Real(value),
                    rusqlite::types::Value::Text(value) => NormalizedValue::Text(value),
                    rusqlite::types::Value::Blob(value) => NormalizedValue::Blob(value),
                });
            }
            Ok(values)
        })
        .map_err(|error| error.to_string())?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())
}

fn fsqlite_value_to_normalized(value: &fsqlite::SqliteValue) -> NormalizedValue {
    match value {
        fsqlite::SqliteValue::Null => NormalizedValue::Null,
        fsqlite::SqliteValue::Integer(value) => NormalizedValue::Integer(*value),
        fsqlite::SqliteValue::Float(value) => NormalizedValue::Real(*value),
        fsqlite::SqliteValue::Text(value) => NormalizedValue::Text(value.to_string()),
        fsqlite::SqliteValue::Blob(value) => NormalizedValue::Blob(value.to_vec()),
    }
}

fn stmt_returns_rows(sql: &str) -> bool {
    sql.split_whitespace().next().is_some_and(|keyword| {
        keyword.eq_ignore_ascii_case("SELECT")
            || keyword.eq_ignore_ascii_case("PRAGMA")
            || keyword.eq_ignore_ascii_case("VALUES")
            || keyword.eq_ignore_ascii_case("WITH")
            || keyword.eq_ignore_ascii_case("EXPLAIN")
    })
}

fn backend_db_path(bundle_dir: &Path, scenario: &ScenarioSpec, backend: BackendKind) -> PathBuf {
    let filename = format!(
        "{}-{}.sqlite3",
        scenario.scenario_id.to_ascii_lowercase(),
        backend.label()
    );
    bundle_dir.join("work").join(filename)
}

fn scenario_matches_filter(scenario: &ScenarioSpec, filter: Option<&str>) -> bool {
    filter.is_none_or(|filter| filter == scenario.scenario_id || filter == "all")
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn default_artifact_root() -> PathBuf {
    env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("target")
        .join("fts5_baseline_artifacts")
}

fn default_run_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    format!("bd-2nzo8-1-3-{}-{}", now.as_secs(), std::process::id())
}

fn now_timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    format!("unix:{}.{:09}", now.as_secs(), now.subsec_nanos())
}

fn current_rss_bytes() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(resident_pages.saturating_mul(4096))
}
