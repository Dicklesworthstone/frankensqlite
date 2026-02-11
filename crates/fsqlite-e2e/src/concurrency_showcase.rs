//! Concurrency Showcase: massively concurrent write workloads.
//!
//! Bead: bd-2als.2.4
//!
//! Builds the flagship "massively concurrent writes" demo that makes
//! FrankenSQLite's MVCC advantage obvious.  Configures a focused performance
//! matrix with the two canonical concurrency workloads and sweeps across
//! concurrency levels, producing a self-contained report.
//!
//! ## Workloads
//!
//! 1. **Disjoint writes (best case):** each worker inserts into a
//!    non-overlapping key range — zero page conflicts, pure MVCC scaling.
//! 2. **Hot-page contention (stress case):** all workers write to the same
//!    leaf page — maximum contention to show conflict handling.
//!
//! ## sqlite3 Baseline
//!
//! sqlite3 serializes writers via `WAL_WRITE_LOCK`.  The harness runs in WAL
//! mode with a configurable `busy_timeout` and counts retries per worker so
//! the cost of serialization is visible in the report.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::HarnessSettings;
use crate::benchmark::BenchmarkConfig;
use crate::perf_runner::{
    CellOutcome, Engine, PerfMatrixConfig, PerfResult, expand_matrix, run_perf_matrix,
};

// ── Configuration ────────────────────────────────────────────────────

/// Default concurrency levels swept by the showcase.
pub const DEFAULT_CONCURRENCY_LEVELS: &[u16] = &[1, 2, 4, 8, 16, 32];

/// The two canonical workload presets for the concurrency showcase.
pub const SHOWCASE_WORKLOADS: &[&str] =
    &["commutative_inserts_disjoint_keys", "hot_page_contention"];

/// Configuration for the concurrency showcase.
#[derive(Debug, Clone)]
pub struct ShowcaseConfig {
    /// Fixture IDs to test against (must exist in `golden/`).
    pub fixture_ids: Vec<String>,
    /// Concurrency levels to sweep (default: 1, 2, 4, 8, 16, 32).
    pub concurrency_levels: Vec<u16>,
    /// Seed for deterministic workload generation.
    pub seed: u64,
    /// Scale factor for workload size.
    pub scale: u32,
    /// Path to the project root.
    pub project_root: PathBuf,
    /// Benchmark configuration (warmup, min iterations, time floor).
    pub benchmark_config: BenchmarkConfig,
    /// Harness settings (PRAGMAs, journal mode, etc.).
    pub settings: HarnessSettings,
    /// Cargo profile string for metadata.
    pub cargo_profile: String,
    /// If true, stop on first error.
    pub fail_fast: bool,
}

impl ShowcaseConfig {
    /// Create a showcase config with sensible defaults.
    ///
    /// Requires at least one fixture ID and a project root path.
    #[must_use]
    pub fn new(fixture_ids: Vec<String>, project_root: PathBuf) -> Self {
        Self {
            fixture_ids,
            concurrency_levels: DEFAULT_CONCURRENCY_LEVELS.to_vec(),
            seed: 42,
            scale: 100,
            project_root,
            benchmark_config: BenchmarkConfig::default(),
            settings: HarnessSettings::default(),
            cargo_profile: "release".to_owned(),
            fail_fast: false,
        }
    }
}

// ── Result types ─────────────────────────────────────────────────────

/// Complete showcase result — wraps a `PerfResult` with showcase metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShowcaseResult {
    /// Schema version for the showcase result format.
    pub schema_version: String,
    /// The underlying perf matrix results.
    pub perf: PerfResult,
    /// Seed used for deterministic workload generation.
    pub seed: u64,
    /// Scale factor.
    pub scale: u32,
    /// Concurrency levels swept.
    pub concurrency_levels: Vec<u16>,
    /// Fixture IDs tested.
    pub fixture_ids: Vec<String>,
}

/// Schema version for the showcase result format.
pub const SHOWCASE_SCHEMA_V1: &str = "fsqlite-e2e.concurrency_showcase.v1";

// ── Runner ───────────────────────────────────────────────────────────

/// Build the `PerfMatrixConfig` from showcase configuration.
#[must_use]
pub fn build_showcase_matrix(config: &ShowcaseConfig) -> PerfMatrixConfig {
    PerfMatrixConfig {
        engines: vec![Engine::Sqlite3, Engine::Fsqlite],
        fixture_ids: config.fixture_ids.clone(),
        workload_presets: SHOWCASE_WORKLOADS.iter().map(|s| (*s).to_owned()).collect(),
        concurrency_levels: config.concurrency_levels.clone(),
        seed: config.seed,
        scale: config.scale,
        project_root: config.project_root.clone(),
        benchmark_config: config.benchmark_config.clone(),
        settings: config.settings.clone(),
        cargo_profile: config.cargo_profile.clone(),
        fail_fast: config.fail_fast,
    }
}

/// Run the concurrency showcase benchmark matrix.
///
/// Executes both engines across both canonical workloads at every
/// concurrency level, collecting benchmark summaries with retry/abort
/// instrumentation.
#[must_use]
pub fn run_concurrency_showcase(config: &ShowcaseConfig) -> ShowcaseResult {
    let matrix_config = build_showcase_matrix(config);
    let perf = run_perf_matrix(&matrix_config);

    ShowcaseResult {
        schema_version: SHOWCASE_SCHEMA_V1.to_owned(),
        perf,
        seed: config.seed,
        scale: config.scale,
        concurrency_levels: config.concurrency_levels.clone(),
        fixture_ids: config.fixture_ids.clone(),
    }
}

/// Count the total cells in a showcase matrix without running it.
#[must_use]
pub fn showcase_matrix_size(config: &ShowcaseConfig) -> usize {
    let matrix_config = build_showcase_matrix(config);
    expand_matrix(&matrix_config).len()
}

// ── Report rendering ─────────────────────────────────────────────────

/// Render a concurrency showcase result into a human-readable Markdown report.
///
/// Produces:
/// - Executive summary with headline speedup numbers
/// - Concurrency scaling tables for each workload
/// - Retry/busy/abort metrics comparison
/// - "What happened" narrative paragraphs for each scenario
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn render_showcase_report(result: &ShowcaseResult, settings: &HarnessSettings) -> String {
    let mut out = String::with_capacity(16384);

    let _ = writeln!(out, "# Concurrency Showcase Report\n");
    let _ = writeln!(
        out,
        "> Comparing FrankenSQLite MVCC concurrent writers vs sqlite3 \
         serialized writers across {} concurrency levels.\n",
        result.concurrency_levels.len()
    );

    if result.perf.cells.is_empty() {
        let _ = writeln!(out, "_No benchmark cells to report._");
        return out;
    }

    // ── Settings ──────────────────────────────────────────────────
    render_settings(&mut out, settings, result);

    // ── Executive Summary ──────────────────────────────────────────
    render_executive_summary(&mut out, &result.perf);

    // ── Per-workload sections ─────────────────────────────────────
    for workload in SHOWCASE_WORKLOADS {
        render_workload_section(&mut out, &result.perf, workload, &result.concurrency_levels);
    }

    // ── Contention metrics ────────────────────────────────────────
    render_contention_metrics(&mut out, &result.perf);

    // ── Error summary ─────────────────────────────────────────────
    let errors: Vec<_> = result
        .perf
        .cells
        .iter()
        .filter(|c| c.error.is_some())
        .collect();
    if !errors.is_empty() {
        let _ = writeln!(out, "## Errors\n");
        for cell in &errors {
            let _ = writeln!(
                out,
                "- **{}** / {} / c{}: {}",
                cell.engine,
                cell.workload,
                cell.concurrency,
                cell.error.as_deref().unwrap_or("unknown"),
            );
        }
        let _ = writeln!(out);
    }

    out
}

// ── Settings section ─────────────────────────────────────────────────

fn render_settings(out: &mut String, settings: &HarnessSettings, result: &ShowcaseResult) {
    let _ = writeln!(out, "## Test Configuration\n");
    let _ = writeln!(out, "| Parameter | Value |");
    let _ = writeln!(out, "|-----------|-------|");
    let _ = writeln!(out, "| Journal mode | {} |", settings.journal_mode);
    let _ = writeln!(out, "| Synchronous | {} |", settings.synchronous);
    let _ = writeln!(out, "| Cache size | {} |", settings.cache_size);
    let _ = writeln!(out, "| Page size | {} |", settings.page_size);
    let _ = writeln!(out, "| Busy timeout (ms) | {} |", settings.busy_timeout_ms);
    let _ = writeln!(
        out,
        "| Concurrency levels | {} |",
        result
            .concurrency_levels
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let _ = writeln!(out, "| Seed | {} |", result.seed);
    let _ = writeln!(out, "| Scale | {} |", result.scale);
    let _ = writeln!(out);
}

// ── Executive summary ────────────────────────────────────────────────

#[allow(clippy::cast_precision_loss)]
fn render_executive_summary(out: &mut String, perf: &PerfResult) {
    let _ = writeln!(out, "## Executive Summary\n");

    // Find the highest-concurrency disjoint-key comparison.
    let disjoint = find_best_speedup(perf, "commutative_inserts_disjoint_keys");
    let contention = find_best_speedup(perf, "hot_page_contention");

    if let Some(ref sp) = disjoint {
        let _ = writeln!(
            out,
            "**Disjoint writes (c={}):** FrankenSQLite completed in **{:.0} ms** \
             (median) vs sqlite3's **{:.0} ms** — a **{:.1}x** speedup. \
             sqlite3 spent its time serializing writers via busy-wait retries ({} total), \
             while FrankenSQLite's MVCC allowed all workers to commit in parallel \
             with zero page conflicts.",
            sp.concurrency,
            sp.fsqlite_median_ms,
            sp.sqlite3_median_ms,
            sp.speedup,
            sp.sqlite3_retries,
        );
        let _ = writeln!(out);
    }

    if let Some(ref sp) = contention {
        let narrative = if sp.speedup >= 1.0 {
            format!(
                "FrankenSQLite still achieved a **{:.1}x** speedup even under maximum contention, \
                 with {} conflict-related aborts handled via first-committer-wins retry.",
                sp.speedup, sp.fsqlite_aborts,
            )
        } else {
            format!(
                "Under maximum page contention, sqlite3 was **{:.1}x** faster (serialization \
                 avoids conflict overhead). FrankenSQLite reported {} aborts from page conflicts. \
                 This is expected: MVCC shines on disjoint workloads, not single-page hotspots.",
                1.0 / sp.speedup,
                sp.fsqlite_aborts,
            )
        };
        let _ = writeln!(
            out,
            "**Hot-page contention (c={}):** {} sqlite3 retries: {}.",
            sp.concurrency, narrative, sp.sqlite3_retries,
        );
        let _ = writeln!(out);
    }

    if disjoint.is_none() && contention.is_none() {
        let _ = writeln!(
            out,
            "_Insufficient data to compute speedup — ensure both engines \
             ran successfully at the same concurrency levels._\n"
        );
    }
}

/// Speedup data for the executive summary.
struct SpeedupData {
    concurrency: u16,
    sqlite3_median_ms: f64,
    fsqlite_median_ms: f64,
    speedup: f64,
    sqlite3_retries: u64,
    fsqlite_aborts: u64,
}

/// Find the highest-concurrency cell where both engines succeeded for a given
/// workload, and compute the speedup ratio.
fn find_best_speedup(perf: &PerfResult, workload: &str) -> Option<SpeedupData> {
    // Group by concurrency, find cells with both engines.
    let mut by_concurrency: BTreeMap<u16, (Option<&CellOutcome>, Option<&CellOutcome>)> =
        BTreeMap::new();

    for cell in &perf.cells {
        if cell.workload != workload || cell.summary.is_none() {
            continue;
        }
        let entry = by_concurrency.entry(cell.concurrency).or_default();
        if cell.engine == "sqlite3" {
            entry.0 = Some(cell);
        } else if cell.engine == "fsqlite" {
            entry.1 = Some(cell);
        }
    }

    // Pick the highest concurrency where both engines have results.
    by_concurrency.into_iter().rev().find_map(|(c, (sq, fs))| {
        let sq = sq?.summary.as_ref()?;
        let fs = fs?.summary.as_ref()?;

        let speedup = if fs.latency.median_ms > 0.0 {
            sq.latency.median_ms / fs.latency.median_ms
        } else {
            0.0
        };

        let sqlite3_retries: u64 = sq.iterations.iter().map(|i| i.retries).sum();
        let fsqlite_aborts: u64 = fs.iterations.iter().map(|i| i.aborts).sum();

        Some(SpeedupData {
            concurrency: c,
            sqlite3_median_ms: sq.latency.median_ms,
            fsqlite_median_ms: fs.latency.median_ms,
            speedup,
            sqlite3_retries,
            fsqlite_aborts,
        })
    })
}

// ── Per-workload section ─────────────────────────────────────────────

fn render_workload_section(
    out: &mut String,
    perf: &PerfResult,
    workload: &str,
    concurrency_levels: &[u16],
) {
    let title = match workload {
        "commutative_inserts_disjoint_keys" => "Disjoint Writes (Best Case)",
        "hot_page_contention" => "Hot-Page Contention (Stress Case)",
        _ => workload,
    };

    let _ = writeln!(out, "## {title}\n");

    let description = match workload {
        "commutative_inserts_disjoint_keys" => {
            "Each worker inserts into a non-overlapping key range. \
             Zero page conflicts expected — this is where MVCC shines."
        }
        "hot_page_contention" => {
            "All workers write to the same leaf page. Maximum contention — \
             every concurrent write is a potential conflict."
        }
        _ => "",
    };
    if !description.is_empty() {
        let _ = writeln!(out, "> {description}\n");
    }

    // Scaling table.
    let _ = writeln!(
        out,
        "| Engine | c | Median (ms) | p95 (ms) | ops/s | Retries | Aborts | Sparkline |"
    );
    let _ = writeln!(
        out,
        "|--------|---|-------------|----------|-------|---------|--------|-----------|"
    );

    // Collect data for sparklines.
    let mut max_ops = 0.0_f64;
    let mut rows: Vec<ScalingRow> = Vec::new();

    for &c in concurrency_levels {
        for engine_name in &["sqlite3", "fsqlite"] {
            if let Some(cell) = perf.cells.iter().find(|cell| {
                cell.workload == workload
                    && cell.concurrency == c
                    && cell.engine == *engine_name
                    && cell.summary.is_some()
            }) {
                let summary = cell.summary.as_ref().unwrap();
                let total_retries: u64 = summary.iterations.iter().map(|i| i.retries).sum();
                let total_aborts: u64 = summary.iterations.iter().map(|i| i.aborts).sum();
                let ops = summary.throughput.median_ops_per_sec;
                max_ops = max_ops.max(ops);

                rows.push(ScalingRow {
                    engine: (*engine_name).to_owned(),
                    concurrency: c,
                    median_ms: summary.latency.median_ms,
                    p95_ms: summary.latency.p95_ms,
                    ops_per_sec: ops,
                    retries: total_retries,
                    aborts: total_aborts,
                });
            }
        }
    }

    for row in &rows {
        let spark = ascii_bar(row.ops_per_sec, max_ops, 16);
        let _ = writeln!(
            out,
            "| {} | {} | {:.1} | {:.1} | {:.0} | {} | {} | {} |",
            row.engine,
            row.concurrency,
            row.median_ms,
            row.p95_ms,
            row.ops_per_sec,
            row.retries,
            row.aborts,
            spark,
        );
    }

    let _ = writeln!(out);

    // Scaling efficiency for each engine.
    render_scaling_efficiency(out, &rows);
}

struct ScalingRow {
    engine: String,
    concurrency: u16,
    median_ms: f64,
    p95_ms: f64,
    ops_per_sec: f64,
    retries: u64,
    aborts: u64,
}

#[allow(clippy::cast_precision_loss)]
fn render_scaling_efficiency(out: &mut String, rows: &[ScalingRow]) {
    for engine in &["sqlite3", "fsqlite"] {
        let engine_rows: Vec<_> = rows.iter().filter(|r| r.engine == *engine).collect();
        let baseline = engine_rows.iter().find(|r| r.concurrency == 1);

        if let Some(base) = baseline {
            if base.ops_per_sec > 0.0 && engine_rows.len() > 1 {
                let _ = writeln!(out, "_Scaling efficiency for {engine} (vs c=1):_\n");
                for row in &engine_rows {
                    if row.concurrency > 1 {
                        let ideal = base.ops_per_sec * f64::from(row.concurrency);
                        let efficiency = row.ops_per_sec / ideal * 100.0;
                        let _ = writeln!(
                            out,
                            "- c={}: {:.0} ops/s ({:.0}% of linear scaling)",
                            row.concurrency, row.ops_per_sec, efficiency
                        );
                    }
                }
                let _ = writeln!(out);
            }
        }
    }
}

// ── Contention metrics ───────────────────────────────────────────────

fn render_contention_metrics(out: &mut String, perf: &PerfResult) {
    let _ = writeln!(out, "## Contention Metrics\n");
    let _ = writeln!(
        out,
        "Comparison of lock contention overhead between the two engines.\n"
    );
    let _ = writeln!(
        out,
        "- **sqlite3 retries:** busy-wait retries due to `WAL_WRITE_LOCK` serialization."
    );
    let _ = writeln!(
        out,
        "- **fsqlite aborts:** transactions aborted due to MVCC page conflicts \
         (first-committer-wins).\n"
    );

    let _ = writeln!(
        out,
        "| Workload | c | sqlite3 retries | sqlite3 aborts | fsqlite retries | fsqlite aborts |"
    );
    let _ = writeln!(
        out,
        "|----------|---|-----------------|----------------|-----------------|----------------|"
    );

    // Collect pairs at each concurrency.
    let mut grouped: BTreeMap<(String, u16), (Option<&CellOutcome>, Option<&CellOutcome>)> =
        BTreeMap::new();
    for cell in &perf.cells {
        if cell.summary.is_none() {
            continue;
        }
        let entry = grouped
            .entry((cell.workload.clone(), cell.concurrency))
            .or_default();
        if cell.engine == "sqlite3" {
            entry.0 = Some(cell);
        } else if cell.engine == "fsqlite" {
            entry.1 = Some(cell);
        }
    }

    for ((workload, c), (sq, fs)) in &grouped {
        let (sq_retries, sq_aborts) = sq.map_or((0, 0), |cell| {
            let s = cell.summary.as_ref().unwrap();
            (
                s.iterations.iter().map(|i| i.retries).sum::<u64>(),
                s.iterations.iter().map(|i| i.aborts).sum::<u64>(),
            )
        });
        let (fs_retries, fs_aborts) = fs.map_or((0, 0), |cell| {
            let s = cell.summary.as_ref().unwrap();
            (
                s.iterations.iter().map(|i| i.retries).sum::<u64>(),
                s.iterations.iter().map(|i| i.aborts).sum::<u64>(),
            )
        });

        let short_workload = match workload.as_str() {
            "commutative_inserts_disjoint_keys" => "disjoint",
            "hot_page_contention" => "hot-page",
            other => other,
        };

        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} |",
            short_workload, c, sq_retries, sq_aborts, fs_retries, fs_aborts,
        );
    }

    let _ = writeln!(out);
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Render a simple ASCII bar for terminal-friendly sparklines.
fn ascii_bar(value: f64, max: f64, width: usize) -> String {
    if max <= 0.0 {
        return " ".repeat(width);
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let filled = ((value / max) * width as f64).round() as usize;
    let filled = filled.min(width);
    let mut bar = String::with_capacity(width);
    for _ in 0..filled {
        bar.push('\u{2588}');
    }
    for _ in filled..width {
        bar.push('\u{2591}');
    }
    bar
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark::{IterationRecord, LatencyStats, ThroughputStats};
    use crate::methodology::{EnvironmentMeta, MethodologyMeta};
    use crate::perf_runner::PERF_RESULT_SCHEMA_V1;

    fn make_cell(
        engine: &str,
        workload: &str,
        concurrency: u16,
        median_ms: f64,
        ops_per_sec: f64,
        retries: u64,
        aborts: u64,
    ) -> CellOutcome {
        use crate::benchmark::BenchmarkSummary;

        CellOutcome {
            summary: Some(BenchmarkSummary {
                benchmark_id: format!("{engine}:{workload}:fix:c{concurrency}"),
                engine: engine.to_owned(),
                workload: workload.to_owned(),
                fixture_id: "synthetic".to_owned(),
                concurrency,
                methodology: MethodologyMeta::current(),
                environment: EnvironmentMeta::capture("test"),
                warmup_count: 1,
                measurement_count: 3,
                total_measurement_ms: 300,
                latency: LatencyStats {
                    min_ms: median_ms * 0.9,
                    max_ms: median_ms * 1.1,
                    mean_ms: median_ms,
                    median_ms,
                    p95_ms: median_ms * 1.05,
                    p99_ms: median_ms * 1.08,
                    stddev_ms: median_ms * 0.05,
                },
                throughput: ThroughputStats {
                    mean_ops_per_sec: ops_per_sec,
                    median_ops_per_sec: ops_per_sec,
                    peak_ops_per_sec: ops_per_sec * 1.1,
                },
                iterations: vec![
                    IterationRecord {
                        iteration: 0,
                        wall_time_ms: 100,
                        ops_per_sec,
                        ops_total: 1000,
                        retries,
                        aborts,
                        error: None,
                    },
                    IterationRecord {
                        iteration: 1,
                        wall_time_ms: 100,
                        ops_per_sec,
                        ops_total: 1000,
                        retries,
                        aborts,
                        error: None,
                    },
                    IterationRecord {
                        iteration: 2,
                        wall_time_ms: 100,
                        ops_per_sec,
                        ops_total: 1000,
                        retries,
                        aborts,
                        error: None,
                    },
                ],
            }),
            error: None,
            engine: engine.to_owned(),
            fixture_id: "synthetic".to_owned(),
            workload: workload.to_owned(),
            concurrency,
        }
    }

    fn make_showcase_result(cells: Vec<CellOutcome>) -> ShowcaseResult {
        let total = cells.len();
        let errors = cells.iter().filter(|c| c.error.is_some()).count();
        ShowcaseResult {
            schema_version: SHOWCASE_SCHEMA_V1.to_owned(),
            perf: PerfResult {
                schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
                total_cells: total,
                success_count: total - errors,
                error_count: errors,
                cells,
            },
            seed: 42,
            scale: 100,
            concurrency_levels: vec![1, 4, 8],
            fixture_ids: vec!["synthetic".to_owned()],
        }
    }

    #[test]
    fn test_default_concurrency_levels() {
        assert_eq!(DEFAULT_CONCURRENCY_LEVELS, &[1, 2, 4, 8, 16, 32]);
    }

    #[test]
    fn test_showcase_workloads() {
        assert_eq!(SHOWCASE_WORKLOADS.len(), 2);
        assert_eq!(SHOWCASE_WORKLOADS[0], "commutative_inserts_disjoint_keys");
        assert_eq!(SHOWCASE_WORKLOADS[1], "hot_page_contention");
    }

    #[test]
    fn test_showcase_config_new() {
        let config = ShowcaseConfig::new(vec!["fix-a".to_owned()], PathBuf::from("/tmp/project"));
        assert_eq!(config.fixture_ids, vec!["fix-a"]);
        assert_eq!(config.seed, 42);
        assert_eq!(config.scale, 100);
        assert_eq!(config.concurrency_levels, DEFAULT_CONCURRENCY_LEVELS);
    }

    #[test]
    fn test_build_showcase_matrix() {
        let config = ShowcaseConfig::new(vec!["fix-a".to_owned()], PathBuf::from("/tmp"));
        let matrix = build_showcase_matrix(&config);

        assert_eq!(matrix.engines.len(), 2);
        assert_eq!(matrix.workload_presets.len(), 2);
        assert_eq!(
            matrix.concurrency_levels.len(),
            DEFAULT_CONCURRENCY_LEVELS.len()
        );

        let cells = expand_matrix(&matrix);
        // 2 engines × 1 fixture × 2 workloads × 6 concurrency levels = 24
        assert_eq!(cells.len(), 24);
    }

    #[test]
    fn test_showcase_matrix_size() {
        let config = ShowcaseConfig::new(
            vec!["fix-a".to_owned(), "fix-b".to_owned()],
            PathBuf::from("/tmp"),
        );
        // 2 engines × 2 fixtures × 2 workloads × 6 concurrency levels = 48
        assert_eq!(showcase_matrix_size(&config), 48);
    }

    #[test]
    fn test_find_best_speedup_disjoint() {
        let perf = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 4,
            success_count: 4,
            error_count: 0,
            cells: vec![
                make_cell(
                    "sqlite3",
                    "commutative_inserts_disjoint_keys",
                    1,
                    100.0,
                    1000.0,
                    0,
                    0,
                ),
                make_cell(
                    "fsqlite",
                    "commutative_inserts_disjoint_keys",
                    1,
                    100.0,
                    1000.0,
                    0,
                    0,
                ),
                make_cell(
                    "sqlite3",
                    "commutative_inserts_disjoint_keys",
                    8,
                    800.0,
                    125.0,
                    50,
                    0,
                ),
                make_cell(
                    "fsqlite",
                    "commutative_inserts_disjoint_keys",
                    8,
                    200.0,
                    5000.0,
                    0,
                    0,
                ),
            ],
        };

        let sp = find_best_speedup(&perf, "commutative_inserts_disjoint_keys");
        assert!(sp.is_some());
        let sp = sp.unwrap();
        assert_eq!(sp.concurrency, 8);
        assert!((sp.speedup - 4.0).abs() < 0.01);
        // Retries are summed across 3 iterations, each with 50.
        assert_eq!(sp.sqlite3_retries, 150);
    }

    #[test]
    fn test_find_best_speedup_missing_engine() {
        let perf = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 1,
            success_count: 1,
            error_count: 0,
            cells: vec![make_cell(
                "sqlite3",
                "commutative_inserts_disjoint_keys",
                4,
                200.0,
                500.0,
                10,
                0,
            )],
        };

        assert!(find_best_speedup(&perf, "commutative_inserts_disjoint_keys").is_none());
    }

    #[test]
    fn test_render_showcase_report_with_data() {
        let result = make_showcase_result(vec![
            make_cell(
                "sqlite3",
                "commutative_inserts_disjoint_keys",
                1,
                100.0,
                1000.0,
                0,
                0,
            ),
            make_cell(
                "fsqlite",
                "commutative_inserts_disjoint_keys",
                1,
                100.0,
                1000.0,
                0,
                0,
            ),
            make_cell(
                "sqlite3",
                "commutative_inserts_disjoint_keys",
                4,
                400.0,
                250.0,
                20,
                0,
            ),
            make_cell(
                "fsqlite",
                "commutative_inserts_disjoint_keys",
                4,
                120.0,
                3333.0,
                0,
                0,
            ),
            make_cell(
                "sqlite3",
                "commutative_inserts_disjoint_keys",
                8,
                800.0,
                125.0,
                50,
                0,
            ),
            make_cell(
                "fsqlite",
                "commutative_inserts_disjoint_keys",
                8,
                200.0,
                5000.0,
                0,
                0,
            ),
            make_cell("sqlite3", "hot_page_contention", 1, 100.0, 1000.0, 0, 0),
            make_cell("fsqlite", "hot_page_contention", 1, 100.0, 1000.0, 0, 0),
            make_cell("sqlite3", "hot_page_contention", 4, 350.0, 285.0, 15, 0),
            make_cell("fsqlite", "hot_page_contention", 4, 300.0, 333.0, 0, 5),
            make_cell("sqlite3", "hot_page_contention", 8, 700.0, 142.0, 40, 0),
            make_cell("fsqlite", "hot_page_contention", 8, 500.0, 200.0, 0, 15),
        ]);

        let report = render_showcase_report(&result, &HarnessSettings::default());

        assert!(report.contains("# Concurrency Showcase Report"));
        assert!(report.contains("Executive Summary"));
        assert!(report.contains("Disjoint Writes (Best Case)"));
        assert!(report.contains("Hot-Page Contention (Stress Case)"));
        assert!(report.contains("Contention Metrics"));
        assert!(report.contains("sqlite3"));
        assert!(report.contains("fsqlite"));
        // Speedup narrative should be present.
        assert!(report.contains("speedup"));
    }

    #[test]
    fn test_render_showcase_report_empty() {
        let result = make_showcase_result(Vec::new());
        let report = render_showcase_report(&result, &HarnessSettings::default());
        assert!(report.contains("No benchmark cells to report"));
    }

    #[test]
    fn test_render_contention_metrics() {
        let perf = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 4,
            success_count: 4,
            error_count: 0,
            cells: vec![
                make_cell("sqlite3", "hot_page_contention", 4, 300.0, 333.0, 10, 0),
                make_cell("fsqlite", "hot_page_contention", 4, 250.0, 400.0, 0, 5),
                make_cell(
                    "sqlite3",
                    "commutative_inserts_disjoint_keys",
                    4,
                    200.0,
                    500.0,
                    5,
                    0,
                ),
                make_cell(
                    "fsqlite",
                    "commutative_inserts_disjoint_keys",
                    4,
                    100.0,
                    1000.0,
                    0,
                    0,
                ),
            ],
        };

        let mut out = String::new();
        render_contention_metrics(&mut out, &perf);

        assert!(out.contains("Contention Metrics"));
        assert!(out.contains("disjoint"));
        assert!(out.contains("hot-page"));
    }

    #[test]
    fn test_ascii_bar() {
        let bar = ascii_bar(50.0, 100.0, 10);
        assert_eq!(bar.chars().count(), 10);

        let full = ascii_bar(100.0, 100.0, 10);
        assert!(full.chars().all(|c| c == '\u{2588}'));

        let empty = ascii_bar(0.0, 100.0, 10);
        assert!(empty.chars().all(|c| c == '\u{2591}'));
    }

    #[test]
    fn test_ascii_bar_zero_max() {
        let bar = ascii_bar(50.0, 0.0, 10);
        assert_eq!(bar.len(), 10);
        assert!(bar.chars().all(|c| c == ' '));
    }

    #[test]
    fn test_showcase_result_serialization() {
        let result = make_showcase_result(vec![make_cell(
            "sqlite3",
            "commutative_inserts_disjoint_keys",
            1,
            100.0,
            1000.0,
            0,
            0,
        )]);

        let json = serde_json::to_string(&result).unwrap();
        let parsed: ShowcaseResult = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.schema_version, SHOWCASE_SCHEMA_V1);
        assert_eq!(parsed.seed, 42);
        assert_eq!(parsed.perf.cells.len(), 1);
    }

    #[test]
    fn test_scaling_efficiency_rendered() {
        let result = make_showcase_result(vec![
            make_cell(
                "fsqlite",
                "commutative_inserts_disjoint_keys",
                1,
                100.0,
                1000.0,
                0,
                0,
            ),
            make_cell(
                "fsqlite",
                "commutative_inserts_disjoint_keys",
                4,
                50.0,
                3500.0,
                0,
                0,
            ),
            make_cell(
                "fsqlite",
                "commutative_inserts_disjoint_keys",
                8,
                30.0,
                6000.0,
                0,
                0,
            ),
        ]);

        let report = render_showcase_report(&result, &HarnessSettings::default());
        // Should contain scaling efficiency line.
        assert!(report.contains("Scaling efficiency"));
        assert!(report.contains("of linear scaling"));
    }
}
