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

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

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
            if let Some(ref summary) = cell.summary {
                out.push_str(&serde_json::to_string(summary)?);
                out.push('\n');
            }
        }
        Ok(out)
    }
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

/// Write benchmark summaries to a JSONL file.
///
/// Each successful cell's `BenchmarkSummary` is written as one JSON line.
///
/// # Errors
///
/// Returns an I/O error if the file cannot be written.
pub fn write_results_jsonl(result: &PerfResult, path: &Path) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    for cell in &result.cells {
        if let Some(ref summary) = cell.summary {
            let line = serde_json::to_string(summary)
                .map_err(std::io::Error::other)?;
            writeln!(file, "{line}")?;
        }
    }
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
}
