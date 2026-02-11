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
use crate::golden;
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
    /// [`ConcurrencySweep::worker_counts`].
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
fn generate_oplog(preset: &str, fixture_id: &str, seed: u64, concurrency: u16) -> Option<OpLog> {
    Some(match preset {
        "commutative_inserts_disjoint_keys" => {
            oplog::preset_commutative_inserts_disjoint_keys(fixture_id, seed, concurrency, 50)
        }
        "hot_page_contention" => {
            oplog::preset_hot_page_contention(fixture_id, seed, concurrency, 20)
        }
        "mixed_read_write" => oplog::preset_mixed_read_write(fixture_id, seed, concurrency, 30),
        "deterministic_transform" => oplog::preset_deterministic_transform(fixture_id, seed, 100),
        "large_txn" => oplog::preset_large_txn(fixture_id, seed, concurrency, 50),
        "schema_migration" => oplog::preset_schema_migration(fixture_id, seed, 100),
        "btree_stress_sequential" => oplog::preset_btree_stress_sequential(fixture_id, seed, 500),
        "wide_row_overflow" => oplog::preset_wide_row_overflow(fixture_id, seed, 20, 8000),
        "bulk_delete_reinsert" => oplog::preset_bulk_delete_reinsert(fixture_id, seed, 200),
        "scatter_write" => oplog::preset_scatter_write(fixture_id, seed, concurrency, 40),
        "multi_table_foreign_keys" => oplog::preset_multi_table_foreign_keys(fixture_id, seed, 50),
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
#[allow(clippy::too_many_lines)]
fn run_cell(
    workspace_config: &WorkspaceConfig,
    fixture_id: &str,
    preset: &PresetMeta,
    concurrency: u16,
    seed: u64,
    settings: &HarnessSettings,
    bundle_config: &mismatch_artifacts::BundleConfig,
    sqlite_config: &SqliteExecConfig,
    fsqlite_config: &FsqliteExecConfig,
) -> CellResult {
    let cell_start = Instant::now();

    // 1. Generate OpLog.
    let Some(oplog) = generate_oplog(&preset.name, fixture_id, seed, concurrency) else {
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
            verdict: CellVerdict::Pass {
                achieved_tier: achieved_tier.clone(),
            },
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
                    detail.push_str(&format!("; repro_bundle_error: {e}"));
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

#[allow(clippy::too_many_lines)]
fn write_mismatch_bundle(
    run_dir: &Path,
    fixture_id: &str,
    preset: &PresetMeta,
    concurrency: u16,
    seed: u64,
    golden_source: &Path,
    oplog: &OpLog,
    sqlite_config: &SqliteExecConfig,
    fsqlite_config: &FsqliteExecConfig,
    sqlite_db: &Path,
    fsqlite_db: &Path,
    comparison: &TieredComparisonResult,
) -> E2eResult<()> {
    let golden_sha256 = golden::GoldenCopy::hash_file(golden_source).ok();

    let meta_path = run_dir.join("mismatch_meta.json");
    #[derive(Serialize)]
    struct MismatchMeta<'a> {
        schema_version: &'a str,
        fixture_id: &'a str,
        preset_name: &'a str,
        concurrency: u16,
        seed: u64,
        golden_source: String,
        golden_sha256: Option<String>,
        sqlite_db: String,
        fsqlite_db: String,
        sqlite_pragmas: Vec<String>,
        sqlite_max_busy_retries: u32,
        fsqlite_pragmas: Vec<String>,
        fsqlite_concurrent_mode: bool,
        expected_tier: String,
        achieved_tier: String,
        tiered_comparison: &'a TieredComparisonResult,
    }

    let meta = MismatchMeta {
        schema_version: "fsqlite-e2e.mismatch_bundle.v1",
        fixture_id,
        preset_name: &preset.name,
        concurrency,
        seed,
        golden_source: golden_source.display().to_string(),
        golden_sha256,
        sqlite_db: sqlite_db.display().to_string(),
        fsqlite_db: fsqlite_db.display().to_string(),
        sqlite_pragmas: sqlite_config.pragmas.clone(),
        sqlite_max_busy_retries: sqlite_config.max_busy_retries,
        fsqlite_pragmas: fsqlite_config.pragmas.clone(),
        fsqlite_concurrent_mode: fsqlite_config.concurrent_mode,
        expected_tier: equiv_tier_to_string(preset.expected_tier),
        achieved_tier: tier_to_string(comparison.tier),
        tiered_comparison: comparison,
    };

    let meta_json = serde_json::to_string_pretty(&meta).map_err(std::io::Error::other)?;
    std::fs::write(&meta_path, meta_json)?;

    // Persist the exact OpLog so the workload is reproducible without regenerating.
    let oplog_json = serde_json::to_string_pretty(oplog).map_err(std::io::Error::other)?;
    std::fs::write(run_dir.join("oplog.json"), oplog_json)?;

    // Golden-style comparison report + diagnostic bundle.
    let (report, diagnostic) = golden::verify_databases(sqlite_db, fsqlite_db)?;
    let report_json = serde_json::to_string_pretty(&report).map_err(std::io::Error::other)?;
    std::fs::write(run_dir.join("comparison_report.json"), report_json)?;

    if let Some(ref diag) = diagnostic {
        let diag_json = serde_json::to_string_pretty(diag).map_err(std::io::Error::other)?;
        std::fs::write(run_dir.join("mismatch_diagnostic.json"), diag_json)?;
        std::fs::write(
            run_dir.join("mismatch_diagnostic.md"),
            golden::format_mismatch_diagnostic(diag),
        )?;
    }

    // Canonicalized outputs (Tier 2 artifacts).
    let _ = canonicalize::canonicalize(sqlite_db, &run_dir.join("canonical_sqlite3.db"))?;
    let _ = canonicalize::canonicalize(fsqlite_db, &run_dir.join("canonical_fsqlite.db"))?;

    // Schema snapshots (sqlite_master DDL).
    write_schema_snapshot(sqlite_db, &run_dir.join("schema_sqlite3.sql"))?;
    write_schema_snapshot(fsqlite_db, &run_dir.join("schema_fsqlite.sql"))?;

    // Deterministic dumps (size-capped).
    write_logical_dump_limited(
        sqlite_db,
        &run_dir.join("dump_sqlite3.txt"),
        5 * 1024 * 1024,
    )?;
    write_logical_dump_limited(
        fsqlite_db,
        &run_dir.join("dump_fsqlite.txt"),
        5 * 1024 * 1024,
    )?;

    // Cross-engine diff files (best-effort — don't fail the whole bundle).
    mismatch_artifacts::write_diff_files(run_dir, sqlite_db, fsqlite_db);

    // Human repro instructions.
    let mut md = String::new();
    use std::fmt::Write as _;
    let _ = writeln!(
        md,
        "# Repro Bundle\n\nFixture: `{}`\nPreset: `{}`\nConcurrency: `{}`\nSeed: `{}`\n",
        fixture_id, preset.name, concurrency, seed
    );
    let _ = writeln!(md, "- Run dir: `{}`", run_dir.display());
    let _ = writeln!(md, "- sqlite3 db: `{}`", sqlite_db.display());
    let _ = writeln!(md, "- fsqlite db: `{}`", fsqlite_db.display());
    let _ = writeln!(md, "- golden source: `{}`", golden_source.display());
    let _ = writeln!(md);
    let _ = writeln!(md, "## Commands\n");
    let _ = writeln!(
        md,
        "1. Compare (JSON):\n   ```bash\n   realdb-e2e compare --db-a \"{}\" --db-b \"{}\" --json\n   ```\n",
        sqlite_db.display(),
        fsqlite_db.display()
    );
    let _ = writeln!(
        md,
        "2. Inspect diagnostic:\n   - `mismatch_diagnostic.md`\n   - `mismatch_diagnostic.json`\n"
    );
    let _ = writeln!(
        md,
        "3. Inspect canonical outputs:\n   - `canonical_sqlite3.db`\n   - `canonical_fsqlite.db`\n"
    );
    let _ = writeln!(
        md,
        "4. Inspect dumps (size-capped):\n   - `dump_sqlite3.txt`\n   - `dump_fsqlite.txt`\n"
    );
    let _ = writeln!(
        md,
        "5. Inspect diffs:\n   - `schema_diff.txt` (schema comparison)\n   - `dump_diff.txt` (row-level differences)\n   - `pragma_diff.txt` (PRAGMA setting differences)\n"
    );

    std::fs::write(run_dir.join("REPRO.md"), md)?;

    Ok(())
}

fn write_schema_snapshot(db_path: &Path, out_path: &Path) -> E2eResult<()> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = rusqlite::Connection::open_with_flags(db_path, flags)?;

    let mut stmt =
        conn.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")?;
    let sqls: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    let mut out = String::new();
    for sql in &sqls {
        out.push_str(sql);
        out.push_str(";\n");
    }
    std::fs::write(out_path, out)?;
    Ok(())
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn write_logical_dump_limited(db_path: &Path, out_path: &Path, max_bytes: usize) -> E2eResult<()> {
    use std::io::Write as _;

    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = rusqlite::Connection::open_with_flags(db_path, flags)?;

    let mut file = std::fs::File::create(out_path)?;
    let mut written = 0usize;

    let mut write_line = |s: &str| -> std::io::Result<bool> {
        if written.saturating_add(s.len()) > max_bytes {
            let note = "\n-- TRUNCATED (max_bytes exceeded) --\n";
            if written.saturating_add(note.len()) <= max_bytes {
                file.write_all(note.as_bytes())?;
                written += note.len();
            }
            return Ok(false);
        }
        file.write_all(s.as_bytes())?;
        written += s.len();
        Ok(true)
    };

    // Schema DDL.
    let mut schema_stmt =
        conn.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")?;
    let mut rows = schema_stmt.query([])?;
    while let Some(row) = rows.next()? {
        let sql: String = row.get(0)?;
        if !write_line(&format!("{sql};\n"))? {
            return Ok(());
        }
    }
    if !write_line("---\n")? {
        return Ok(());
    }

    // Tables + rows.
    let mut table_stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let tables: Vec<String> = table_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for table in &tables {
        if !write_line(&format!("TABLE {table}\n"))? {
            return Ok(());
        }
        let q = format!("SELECT * FROM {} ORDER BY 1", quote_ident(table));
        let mut stmt = conn.prepare(&q)?;
        let col_count = stmt.column_count();
        let mut r = stmt.query([])?;
        while let Some(row) = r.next()? {
            let mut parts = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let v: rusqlite::types::Value = row.get(i)?;
                parts.push(format!("{v:?}"));
            }
            if !write_line(&format!("  {}\n", parts.join("|")))? {
                return Ok(());
            }
        }
    }

    Ok(())
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
            let log = generate_oplog(&meta.name, "test_fixture", 42, 2);
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
        assert!(generate_oplog("nonexistent_preset", "fix", 1, 1).is_none());
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
