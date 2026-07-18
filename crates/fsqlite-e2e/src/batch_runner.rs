//! Batch runner: execute workload matrix across fixtures and assert tiered hashes.
//!
//! Bead: bd-2als.3.3
//!
//! # Overview
//!
//! Given a matrix of `(fixture × workload_preset × concurrency × seed)`,
//! the batch runner:
//!
//! 1. Creates an isolated workspace per cell (copies golden DB).
//! 2. Generates the deterministic OpLog from the preset + seed.
//! 3. Runs the OpLog against C SQLite (rusqlite) **and** FrankenSQLite.
//! 4. Compares outputs using the three-tier canonicalization pipeline.
//! 5. Asserts the result meets the preset's declared [`EquivalenceTier`].
//! 6. Collects all results into a [`BatchResult`] with JSONL serialization.
//!
//! # Fail modes
//!
//! - **CI mode** (`fail_fast = true`): stop on first divergence.
//! - **Local mode** (`fail_fast = false`): run all cells, report everything.

use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::canonicalize::{self, ComparisonTier, TieredComparisonResult};
use crate::fsqlite_executor::{self, FsqliteExecConfig};
use crate::mismatch_artifacts;
use crate::oplog::{self, EquivalenceTier, OpLog, PresetMeta};
use crate::report::EngineRunReport;
use crate::run_workspace::{self, WorkspaceConfig};
use crate::sqlite_executor::{self, SqliteExecConfig};
use crate::{E2eError, E2eResult, HarnessSettings};

// ── Configuration ──────────────────────────────────────────────────────

/// Configuration for a batch matrix run.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Project root for locating golden fixtures and working directories.
    pub project_root: PathBuf,
    /// Fixture DB identifiers to test.  Empty means "all golden fixtures".
    pub fixture_ids: Vec<String>,
    /// Preset names from [`oplog::preset_catalog()`].  Empty means "all".
    pub preset_names: Vec<String>,
    /// Concurrency levels to sweep.  Empty means use each preset's
    /// [`field@crate::oplog::ConcurrencySweep::worker_counts`].
    pub concurrency_levels: Vec<u16>,
    /// Seeds for deterministic replay.
    pub seeds: Vec<u64>,
    /// Default scale factor for preset row/op counts (multiplied against
    /// the preset's default scale).  `1` = default, `0` = minimum.
    pub scale: u32,
    /// Harness settings (PRAGMAs, journal mode, etc.) applied to both engines.
    pub settings: HarnessSettings,
    /// Stop on first mismatch?
    pub fail_fast: bool,
    /// Configuration for mismatch artifact bundles.
    pub bundle_config: mismatch_artifacts::BundleConfig,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            project_root: PathBuf::from("."),
            fixture_ids: Vec::new(),
            preset_names: Vec::new(),
            concurrency_levels: Vec::new(),
            seeds: vec![42],
            scale: 1,
            settings: HarnessSettings::default(),
            fail_fast: false,
            bundle_config: mismatch_artifacts::BundleConfig::default(),
        }
    }
}

// ── Cell-level result ──────────────────────────────────────────────────

/// Verdict for a single matrix cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CellVerdict {
    /// Both engines agree at or above the expected tier.
    Pass {
        /// The tier that was achieved.
        achieved_tier: String,
    },
    /// Engines disagree or achieved a weaker tier than expected.
    Mismatch {
        expected_tier: String,
        achieved_tier: Option<String>,
        detail: String,
    },
    /// An execution or infrastructure error prevented comparison.
    Error(String),
}

/// Result of a single `(fixture × preset × concurrency × seed)` cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellResult {
    pub fixture_id: String,
    pub preset_name: String,
    pub concurrency: u16,
    pub seed: u64,
    pub wall_time_ms: u64,
    /// Directory containing any mismatch repro artifacts for this cell.
    ///
    /// Present only when a mismatch (or late-stage error) occurred.
    pub artifact_dir: Option<String>,
    pub sqlite_report: Option<EngineRunReport>,
    pub fsqlite_report: Option<EngineRunReport>,
    pub tiered_comparison: Option<TieredComparisonResult>,
    pub verdict: CellVerdict,
}

// ── Batch-level result ─────────────────────────────────────────────────

/// Aggregated results from a full batch matrix run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResult {
    pub schema_version: String,
    pub total_cells: usize,
    pub pass_count: usize,
    pub mismatch_count: usize,
    pub error_count: usize,
    pub cells: Vec<CellResult>,
}

impl BatchResult {
    /// `true` if every cell passed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.mismatch_count == 0 && self.error_count == 0
    }

    /// Serialize the batch result as a JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Serialize each cell as a JSONL line.
    pub fn to_jsonl(&self) -> Result<String, serde_json::Error> {
        let mut buf = String::new();
        for cell in &self.cells {
            let line = serde_json::to_string(cell)?;
            buf.push_str(&line);
            buf.push('\n');
        }
        Ok(buf)
    }
}

/// Convert elapsed duration to milliseconds as u64 (saturating).
fn elapsed_ms(start: Instant) -> u64 {
    let ms = start.elapsed().as_millis();
    u64::try_from(ms).unwrap_or(u64::MAX)
}

// ── Preset dispatch ────────────────────────────────────────────────────

/// Default scale values per preset.  Each returns a reasonable small-test
/// value suitable for CI smoke; multiply by `BatchConfig::scale` for
/// longer runs.
fn generate_oplog(
    preset: &str,
    fixture_id: &str,
    seed: u64,
    concurrency: u16,
    scale: u32,
) -> Option<OpLog> {
    let scale_multiplier = scale.max(1);
    let scaled = |base: u32| base.saturating_mul(scale_multiplier).max(1);

    Some(match preset {
        "commutative_inserts_disjoint_keys" => oplog::preset_commutative_inserts_disjoint_keys(
            fixture_id,
            seed,
            concurrency,
            scaled(50),
        ),
        "hot_page_contention" => {
            oplog::preset_hot_page_contention(fixture_id, seed, concurrency, scaled(20))
        }
        "mixed_read_write" => {
            oplog::preset_mixed_read_write(fixture_id, seed, concurrency, scaled(30))
        }
        "deterministic_transform" => {
            oplog::preset_deterministic_transform(fixture_id, seed, scaled(100))
        }
        "large_txn" => oplog::preset_large_txn(fixture_id, seed, concurrency, scaled(50)),
        "schema_migration" => oplog::preset_schema_migration(fixture_id, seed, scaled(100)),
        "btree_stress_sequential" => {
            oplog::preset_btree_stress_sequential(fixture_id, seed, scaled(500))
        }
        "wide_row_overflow" => oplog::preset_wide_row_overflow(fixture_id, seed, scaled(20), 8000),
        "bulk_delete_reinsert" => oplog::preset_bulk_delete_reinsert(fixture_id, seed, scaled(200)),
        "scatter_write" => oplog::preset_scatter_write(fixture_id, seed, concurrency, scaled(40)),
        "multi_table_foreign_keys" => {
            oplog::preset_multi_table_foreign_keys(fixture_id, seed, scaled(50))
        }
        _ => return None,
    })
}

// ── Tier satisfaction check ────────────────────────────────────────────

/// Check whether `achieved` satisfies the `expected` equivalence tier.
///
/// The tier hierarchy is Tier1Raw > Tier2Canonical > Tier3Logical.
/// Achieving a *stronger* tier than expected is always a pass.
fn tier_satisfies(expected: EquivalenceTier, comparison: &TieredComparisonResult) -> bool {
    // A comparison result of ByteIdentical satisfies any expected tier.
    // LogicalMatch satisfies Tier2 or Tier3.
    // DataComplete satisfies Tier3 only.
    match expected {
        EquivalenceTier::Tier1Raw => comparison.byte_match,
        EquivalenceTier::Tier2Canonical => comparison.byte_match || comparison.logical_match,
        EquivalenceTier::Tier3Logical => {
            comparison.byte_match || comparison.logical_match || comparison.row_counts_match
        }
    }
}

fn tier_to_string(tier: ComparisonTier) -> String {
    tier.to_string()
}

fn equiv_tier_to_string(tier: EquivalenceTier) -> String {
    match tier {
        EquivalenceTier::Tier1Raw => "Tier1Raw".to_owned(),
        EquivalenceTier::Tier2Canonical => "Tier2Canonical".to_owned(),
        EquivalenceTier::Tier3Logical => "Tier3Logical".to_owned(),
    }
}

// ── Single-cell execution ──────────────────────────────────────────────

/// Execute a single matrix cell: run both engines against the same fixture
/// and workload, then compare results.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn run_cell(
    workspace_config: &WorkspaceConfig,
    fixture_id: &str,
    preset: &PresetMeta,
    concurrency: u16,
    seed: u64,
    scale: u32,
    settings: &HarnessSettings,
    bundle_config: &mismatch_artifacts::BundleConfig,
    sqlite_config: &SqliteExecConfig,
    fsqlite_config: &FsqliteExecConfig,
) -> CellResult {
    let cell_start = Instant::now();

    // 1. Generate OpLog.
    let Some(oplog) = generate_oplog(&preset.name, fixture_id, seed, concurrency, scale) else {
        return CellResult {
            fixture_id: fixture_id.to_owned(),
            preset_name: preset.name.clone(),
            concurrency,
            seed,
            wall_time_ms: elapsed_ms(cell_start),
            artifact_dir: None,
            sqlite_report: None,
            fsqlite_report: None,
            tiered_comparison: None,
            verdict: CellVerdict::Error(format!("unknown preset: {}", preset.name)),
        };
    };

    // 2. Create isolated workspace with two DB copies.
    let Ok(workspace) = run_workspace::create_workspace_with_label(
        workspace_config,
        &[fixture_id],
        &format!(
            "batch_{}_{}_c{}_s{}",
            preset.name, fixture_id, concurrency, seed
        ),
    ) else {
        return make_error_cell(
            fixture_id,
            &preset.name,
            concurrency,
            seed,
            cell_start,
            None,
            "workspace creation failed".to_owned(),
        );
    };

    // Find the DB path in the workspace.
    let Some(db_entry) = workspace.databases.iter().find(|d| d.db_id == fixture_id) else {
        return make_error_cell(
            fixture_id,
            &preset.name,
            concurrency,
            seed,
            cell_start,
            Some(workspace.run_dir.display().to_string()),
            format!("fixture {fixture_id} not found in workspace"),
        );
    };

    // Create two separate copies: one for each engine.
    let sqlite_db = workspace.run_dir.join(format!("{fixture_id}_sqlite3.db"));
    let fsqlite_db = workspace.run_dir.join(format!("{fixture_id}_fsqlite.db"));

    if let Err(e) = std::fs::copy(&db_entry.db_path, &sqlite_db) {
        return make_error_cell(
            fixture_id,
            &preset.name,
            concurrency,
            seed,
            cell_start,
            Some(workspace.run_dir.display().to_string()),
            format!("copy for sqlite3 failed: {e}"),
        );
    }
    if let Err(e) = std::fs::copy(&db_entry.db_path, &fsqlite_db) {
        return make_error_cell(
            fixture_id,
            &preset.name,
            concurrency,
            seed,
            cell_start,
            Some(workspace.run_dir.display().to_string()),
            format!("copy for fsqlite failed: {e}"),
        );
    }

    // 3. Run C SQLite.
    let sqlite_result = sqlite_executor::run_oplog_sqlite(&sqlite_db, &oplog, sqlite_config);
    let sqlite_report = match sqlite_result {
        Ok(r) => r,
        Err(e) => {
            return make_error_cell(
                fixture_id,
                &preset.name,
                concurrency,
                seed,
                cell_start,
                Some(workspace.run_dir.display().to_string()),
                format!("sqlite executor error: {e}"),
            );
        }
    };

    // 4. Run FrankenSQLite.
    let fsqlite_result = fsqlite_executor::run_oplog_fsqlite(&fsqlite_db, &oplog, fsqlite_config);
    let fsqlite_report = match fsqlite_result {
        Ok(r) => r,
        Err(e) => {
            return CellResult {
                fixture_id: fixture_id.to_owned(),
                preset_name: preset.name.clone(),
                concurrency,
                seed,
                wall_time_ms: elapsed_ms(cell_start),
                artifact_dir: Some(workspace.run_dir.display().to_string()),
                sqlite_report: Some(sqlite_report),
                fsqlite_report: None,
                tiered_comparison: None,
                verdict: CellVerdict::Error(format!("fsqlite executor error: {e}")),
            };
        }
    };

    // 5. Three-tier comparison.
    let comparison = match canonicalize::canonicalize_and_compare(&sqlite_db, &fsqlite_db) {
        Ok(c) => c,
        Err(e) => {
            return CellResult {
                fixture_id: fixture_id.to_owned(),
                preset_name: preset.name.clone(),
                concurrency,
                seed,
                wall_time_ms: elapsed_ms(cell_start),
                artifact_dir: Some(workspace.run_dir.display().to_string()),
                sqlite_report: Some(sqlite_report),
                fsqlite_report: Some(fsqlite_report),
                tiered_comparison: None,
                verdict: CellVerdict::Error(format!("comparison failed: {e}")),
            };
        }
    };

    // 6. Assert tier satisfaction.
    let expected_tier = equiv_tier_to_string(preset.expected_tier);
    let achieved_tier = tier_to_string(comparison.tier);
    let comparison_detail = comparison.detail.clone();

    let mut cell = if tier_satisfies(preset.expected_tier, &comparison) {
        CellResult {
            fixture_id: fixture_id.to_owned(),
            preset_name: preset.name.clone(),
            concurrency,
            seed,
            wall_time_ms: elapsed_ms(cell_start),
            artifact_dir: None,
            sqlite_report: Some(sqlite_report),
            fsqlite_report: Some(fsqlite_report),
            tiered_comparison: Some(comparison),
            verdict: CellVerdict::Pass { achieved_tier },
        }
    } else {
        CellResult {
            fixture_id: fixture_id.to_owned(),
            preset_name: preset.name.clone(),
            concurrency,
            seed,
            wall_time_ms: elapsed_ms(cell_start),
            artifact_dir: None,
            sqlite_report: Some(sqlite_report),
            fsqlite_report: Some(fsqlite_report),
            tiered_comparison: Some(comparison),
            verdict: CellVerdict::Mismatch {
                expected_tier,
                achieved_tier: Some(achieved_tier),
                detail: comparison_detail,
            },
        }
    };

    if matches!(cell.verdict, CellVerdict::Mismatch { .. }) {
        cell.artifact_dir = Some(workspace.run_dir.display().to_string());
        match mismatch_artifacts::write_mismatch_bundle(
            &cell,
            &sqlite_db,
            &fsqlite_db,
            Some(&db_entry.golden_source),
            settings,
            bundle_config,
        ) {
            Ok(bundle_dir) => {
                cell.artifact_dir = Some(bundle_dir.display().to_string());
            }
            Err(e) => {
                if let CellVerdict::Mismatch { detail, .. } = &mut cell.verdict {
                    let _ = std::fmt::Write::write_fmt(
                        detail,
                        format_args!("; repro_bundle_error: {e}"),
                    );
                }
            }
        }
    }

    cell
}

fn make_error_cell(
    fixture_id: &str,
    preset_name: &str,
    concurrency: u16,
    seed: u64,
    start: Instant,
    artifact_dir: Option<String>,
    error: String,
) -> CellResult {
    CellResult {
        fixture_id: fixture_id.to_owned(),
        preset_name: preset_name.to_owned(),
        concurrency,
        seed,
        wall_time_ms: elapsed_ms(start),
        artifact_dir,
        sqlite_report: None,
        fsqlite_report: None,
        tiered_comparison: None,
        verdict: CellVerdict::Error(error),
    }
}

// ── Matrix orchestrator ────────────────────────────────────────────────

/// Execute the full batch matrix and collect results.
///
/// Iterates over `fixture_ids × preset_names × concurrency_levels × seeds`,
/// running each combination through both engines and comparing outputs.
///
/// # Errors
///
/// Returns `E2eError` only for fatal infrastructure failures (e.g. cannot
/// create working directory root).  Per-cell failures are captured in the
/// [`CellResult::verdict`].
#[allow(clippy::too_many_lines)]
pub fn run_matrix(config: &BatchConfig) -> E2eResult<BatchResult> {
    let workspace_config = WorkspaceConfig::from_project_root(&config.project_root);

    // Resolve catalog.
    let catalog = oplog::preset_catalog();
    let presets: Vec<&PresetMeta> = if config.preset_names.is_empty() {
        catalog.iter().collect()
    } else {
        config
            .preset_names
            .iter()
            .filter_map(|name| catalog.iter().find(|p| &p.name == name))
            .collect()
    };

    if presets.is_empty() {
        return Err(E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no matching presets found in catalog",
        )));
    }

    // Resolve fixture list.  If empty, discover from golden dir.
    let fixture_ids: Vec<String> = if config.fixture_ids.is_empty() {
        discover_fixture_ids(&workspace_config.golden_dir)?
    } else {
        config.fixture_ids.clone()
    };

    if fixture_ids.is_empty() {
        return Err(E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no golden fixtures found in {}",
                workspace_config.golden_dir.display()
            ),
        )));
    }

    // Build engine configs from harness settings.
    let sqlite_config = config.settings.to_sqlite_exec_config();
    let fsqlite_config = config.settings.to_fsqlite_exec_config();

    let mut cells = Vec::new();
    let mut pass_count = 0usize;
    let mut mismatch_count = 0usize;
    let mut error_count = 0usize;

    for fixture_id in &fixture_ids {
        for preset in &presets {
            // Resolve concurrency levels: use config if provided, otherwise
            // use the preset's declared sweep.
            let concurrency_levels: &[u16] = if config.concurrency_levels.is_empty() {
                &preset.concurrency_sweep.worker_counts
            } else {
                &config.concurrency_levels
            };

            for &concurrency in concurrency_levels {
                for &seed in &config.seeds {
                    let cell = run_cell(
                        &workspace_config,
                        fixture_id,
                        preset,
                        concurrency,
                        seed,
                        config.scale,
                        &config.settings,
                        &config.bundle_config,
                        &sqlite_config,
                        &fsqlite_config,
                    );

                    match &cell.verdict {
                        CellVerdict::Pass { .. } => pass_count += 1,
                        CellVerdict::Mismatch { .. } => {
                            mismatch_count += 1;
                            if config.fail_fast {
                                cells.push(cell);
                                return Ok(BatchResult {
                                    schema_version: "fsqlite-e2e.batch.v1".to_owned(),
                                    total_cells: cells.len(),
                                    pass_count,
                                    mismatch_count,
                                    error_count,
                                    cells,
                                });
                            }
                        }
                        CellVerdict::Error(_) => {
                            error_count += 1;
                            if config.fail_fast {
                                cells.push(cell);
                                return Ok(BatchResult {
                                    schema_version: "fsqlite-e2e.batch.v1".to_owned(),
                                    total_cells: cells.len(),
                                    pass_count,
                                    mismatch_count,
                                    error_count,
                                    cells,
                                });
                            }
                        }
                    }

                    cells.push(cell);
                }
            }
        }
    }

    Ok(BatchResult {
        schema_version: "fsqlite-e2e.batch.v1".to_owned(),
        total_cells: cells.len(),
        pass_count,
        mismatch_count,
        error_count,
        cells,
    })
}

/// Discover fixture identifiers from the golden directory.
fn discover_fixture_ids(golden_dir: &Path) -> E2eResult<Vec<String>> {
    if !golden_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(golden_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("db") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                ids.push(stem.to_owned());
            }
        }
    }
    ids.sort();
    Ok(ids)
}

// ── Markdown summary ───────────────────────────────────────────────────

/// Render a human-readable markdown summary of a batch result.
#[must_use]
pub fn render_batch_summary(result: &BatchResult) -> String {
    use std::fmt::Write as _;

    let mut md = String::new();

    md.push_str("# Batch Runner Results\n\n");
    let _ = writeln!(
        md,
        "**Total cells:** {} | **Pass:** {} | **Mismatch:** {} | **Error:** {}\n",
        result.total_cells, result.pass_count, result.mismatch_count, result.error_count
    );

    if result.all_passed() {
        md.push_str("> All cells passed.\n\n");
    }

    // Summary table.
    md.push_str("| Fixture | Preset | Conc | Seed | Verdict | Tier | Time (ms) |\n");
    md.push_str("|---------|--------|------|------|---------|------|-----------|\n");

    for cell in &result.cells {
        let (verdict_str, tier_str) = match &cell.verdict {
            CellVerdict::Pass { achieved_tier } => ("PASS", achieved_tier.clone()),
            CellVerdict::Mismatch { achieved_tier, .. } => (
                "MISMATCH",
                achieved_tier.as_deref().unwrap_or("none").to_owned(),
            ),
            CellVerdict::Error(msg) => ("ERROR", msg.chars().take(40).collect()),
        };
        let _ = writeln!(
            md,
            "| {} | {} | {} | {} | {} | {} | {} |",
            cell.fixture_id,
            cell.preset_name,
            cell.concurrency,
            cell.seed,
            verdict_str,
            tier_str,
            cell.wall_time_ms,
        );
    }

    // Detail mismatches.
    let mismatches: Vec<&CellResult> = result
        .cells
        .iter()
        .filter(|c| matches!(c.verdict, CellVerdict::Mismatch { .. }))
        .collect();

    if !mismatches.is_empty() {
        md.push_str("\n## Mismatches\n\n");
        for cell in mismatches {
            if let CellVerdict::Mismatch {
                expected_tier,
                achieved_tier,
                detail,
            } = &cell.verdict
            {
                let _ = writeln!(
                    md,
                    "### {} / {} (conc={}, seed={})\n",
                    cell.fixture_id, cell.preset_name, cell.concurrency, cell.seed
                );
                let _ = writeln!(md, "- **Expected tier:** {expected_tier}");
                let _ = writeln!(
                    md,
                    "- **Achieved tier:** {}",
                    achieved_tier.as_deref().unwrap_or("none")
                );
                let _ = writeln!(md, "- **Detail:** {detail}\n");
            }
        }
    }

    md
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_oplog_all_presets() {
        let catalog = oplog::preset_catalog();
        for meta in &catalog {
            let log = generate_oplog(&meta.name, "test_fixture", 42, 2, 1);
            assert!(
                log.is_some(),
                "preset {} not handled by generate_oplog dispatch",
                meta.name
            );
            let log = log.unwrap();
            assert!(
                !log.records.is_empty(),
                "preset {} generated empty OpLog",
                meta.name
            );
        }
    }

    #[test]
    fn test_generate_oplog_unknown_returns_none() {
        assert!(generate_oplog("nonexistent_preset", "fix", 1, 1, 1).is_none());
    }

    #[test]
    fn test_generate_oplog_scale_multiplier_applies() {
        let small =
            generate_oplog("deterministic_transform", "fix", 1, 1, 1).expect("preset should exist");
        let large =
            generate_oplog("deterministic_transform", "fix", 1, 1, 3).expect("preset should exist");

        assert!(
            large.records.len() > small.records.len(),
            "higher scale should generate larger workloads"
        );
    }

    #[test]
    fn test_tier_satisfies_tier1() {
        let comp = TieredComparisonResult {
            tier: ComparisonTier::ByteIdentical,
            sha256_a: Some("abc".to_owned()),
            sha256_b: Some("abc".to_owned()),
            byte_match: true,
            logical_match: true,
            row_counts_match: true,
            detail: "match".to_owned(),
        };
        assert!(tier_satisfies(EquivalenceTier::Tier1Raw, &comp));
        assert!(tier_satisfies(EquivalenceTier::Tier2Canonical, &comp));
        assert!(tier_satisfies(EquivalenceTier::Tier3Logical, &comp));
    }

    #[test]
    fn test_tier_satisfies_tier2_only() {
        let comp = TieredComparisonResult {
            tier: ComparisonTier::LogicalMatch,
            sha256_a: Some("abc".to_owned()),
            sha256_b: Some("def".to_owned()),
            byte_match: false,
            logical_match: true,
            row_counts_match: true,
            detail: "logical match".to_owned(),
        };
        assert!(!tier_satisfies(EquivalenceTier::Tier1Raw, &comp));
        assert!(tier_satisfies(EquivalenceTier::Tier2Canonical, &comp));
        assert!(tier_satisfies(EquivalenceTier::Tier3Logical, &comp));
    }

    #[test]
    fn test_tier_satisfies_tier3_only() {
        let comp = TieredComparisonResult {
            tier: ComparisonTier::DataComplete,
            sha256_a: None,
            sha256_b: None,
            byte_match: false,
            logical_match: false,
            row_counts_match: true,
            detail: "counts match".to_owned(),
        };
        assert!(!tier_satisfies(EquivalenceTier::Tier1Raw, &comp));
        assert!(!tier_satisfies(EquivalenceTier::Tier2Canonical, &comp));
        assert!(tier_satisfies(EquivalenceTier::Tier3Logical, &comp));
    }

    #[test]
    fn test_batch_result_all_passed() {
        let result = BatchResult {
            schema_version: "fsqlite-e2e.batch.v1".to_owned(),
            total_cells: 2,
            pass_count: 2,
            mismatch_count: 0,
            error_count: 0,
            cells: vec![],
        };
        assert!(result.all_passed());
    }

    #[test]
    fn test_batch_result_has_failure() {
        let result = BatchResult {
            schema_version: "fsqlite-e2e.batch.v1".to_owned(),
            total_cells: 3,
            pass_count: 2,
            mismatch_count: 1,
            error_count: 0,
            cells: vec![],
        };
        assert!(!result.all_passed());
    }

    #[test]
    fn test_batch_result_jsonl_serialization() {
        let cell = CellResult {
            fixture_id: "test".to_owned(),
            preset_name: "deterministic_transform".to_owned(),
            concurrency: 1,
            seed: 42,
            wall_time_ms: 100,
            artifact_dir: None,
            sqlite_report: None,
            fsqlite_report: None,
            tiered_comparison: None,
            verdict: CellVerdict::Pass {
                achieved_tier: "Tier 1: Byte-Identical (SHA-256)".to_owned(),
            },
        };
        let result = BatchResult {
            schema_version: "fsqlite-e2e.batch.v1".to_owned(),
            total_cells: 1,
            pass_count: 1,
            mismatch_count: 0,
            error_count: 0,
            cells: vec![cell],
        };
        let jsonl = result.to_jsonl().unwrap();
        assert!(!jsonl.is_empty());
        assert!(jsonl.contains("deterministic_transform"));
    }

    #[test]
    fn test_render_batch_summary_empty() {
        let result = BatchResult {
            schema_version: "fsqlite-e2e.batch.v1".to_owned(),
            total_cells: 0,
            pass_count: 0,
            mismatch_count: 0,
            error_count: 0,
            cells: vec![],
        };
        let md = render_batch_summary(&result);
        assert!(md.contains("Batch Runner Results"));
        assert!(md.contains("All cells passed"));
    }

    #[test]
    fn test_render_batch_summary_with_mismatch() {
        let cell = CellResult {
            fixture_id: "sample".to_owned(),
            preset_name: "hot_page_contention".to_owned(),
            concurrency: 4,
            seed: 99,
            wall_time_ms: 250,
            artifact_dir: None,
            sqlite_report: None,
            fsqlite_report: None,
            tiered_comparison: None,
            verdict: CellVerdict::Mismatch {
                expected_tier: "Tier2Canonical".to_owned(),
                achieved_tier: Some("Tier 3: Data Complete".to_owned()),
                detail: "canonical SHA-256 differs".to_owned(),
            },
        };
        let result = BatchResult {
            schema_version: "fsqlite-e2e.batch.v1".to_owned(),
            total_cells: 1,
            pass_count: 0,
            mismatch_count: 1,
            error_count: 0,
            cells: vec![cell],
        };
        let md = render_batch_summary(&result);
        assert!(md.contains("MISMATCH"));
        assert!(md.contains("Mismatches"));
        assert!(md.contains("canonical SHA-256 differs"));
    }
}
