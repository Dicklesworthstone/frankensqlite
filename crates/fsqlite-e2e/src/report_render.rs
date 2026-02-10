//! Report rendering: JSONL run records -> human-readable markdown summary.
//!
//! Bead: bd-1w6k.6.3, bd-2als.2.3
//!
//! Reads JSONL files containing [`RunRecordV1`] or [`BenchmarkSummary`]
//! records and renders them into a self-contained Markdown document with
//! comparison tables and key statistics.
//!
//! ## Usage
//!
//! ```text
//! let records = parse_run_records_jsonl(&jsonl_content)?;
//! let markdown = render_run_records_markdown(&records);
//! std::fs::write("reports/summary.md", markdown)?;
//! ```

use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::Path;

use crate::HarnessSettings;
use crate::benchmark::BenchmarkSummary;
use crate::perf_runner::{CellOutcome, PerfResult};
use crate::report::RunRecordV1;

type FixtureWorkloadConcurrencyKey = (String, String, u16);
type PerfByFixture<'a> = BTreeMap<&'a str, Vec<(&'a str, u16, Vec<&'a CellOutcome>)>>;

// ── Parsing ────────────────────────────────────────────────────────────

/// Parse a JSONL string into a list of [`RunRecordV1`] records.
///
/// Blank lines and lines that fail to parse are silently skipped.
#[must_use]
pub fn parse_run_records_jsonl(jsonl: &str) -> Vec<RunRecordV1> {
    jsonl
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// Parse a JSONL string into a list of [`BenchmarkSummary`] records.
///
/// Blank lines and lines that fail to parse are silently skipped.
#[must_use]
pub fn parse_benchmark_summaries_jsonl(jsonl: &str) -> Vec<BenchmarkSummary> {
    jsonl
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// Parse a JSONL string of [`CellOutcome`] records.
///
/// Blank lines and lines that fail to parse are silently skipped.
#[must_use]
pub fn parse_perf_result_jsonl(jsonl: &str) -> Vec<CellOutcome> {
    jsonl
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

// ── Markdown rendering from RunRecordV1 ────────────────────────────────

/// Render a list of [`RunRecordV1`] records into a Markdown summary.
///
/// Groups records by `(fixture_id, workload, concurrency)` and produces
/// a comparison table showing each engine's performance side by side.
#[must_use]
pub fn render_run_records_markdown(records: &[RunRecordV1]) -> String {
    let mut out = String::with_capacity(4096);

    let _ = writeln!(out, "# E2E Run Report\n");

    if records.is_empty() {
        let _ = writeln!(out, "_No records to report._");
        return out;
    }

    // Capture methodology from the first record.
    let meth = &records[0].methodology;
    let _ = writeln!(out, "## Methodology\n");
    let _ = writeln!(out, "- **Version:** {}", meth.version);
    let _ = writeln!(out, "- **Warmup iterations:** {}", meth.warmup_iterations);
    let _ = writeln!(
        out,
        "- **Measurement iterations:** ≥ {}",
        meth.min_measurement_iterations
    );
    let _ = writeln!(
        out,
        "- **Measurement time:** ≥ {} s",
        meth.measurement_time_secs
    );
    let _ = writeln!(out, "- **Primary statistic:** {}", meth.primary_statistic);
    let _ = writeln!(out, "- **Tail statistic:** {}", meth.tail_statistic);
    let _ = writeln!(
        out,
        "- **Fresh DB per iteration:** {}",
        meth.fresh_db_per_iteration
    );
    let _ = writeln!(
        out,
        "- **Identical PRAGMAs enforced:** {}\n",
        meth.identical_pragmas_enforced
    );

    // Group by (fixture_id, workload, concurrency).
    let mut groups: BTreeMap<FixtureWorkloadConcurrencyKey, Vec<&RunRecordV1>> = BTreeMap::new();
    for record in records {
        let key = (
            record.fixture_id.clone(),
            record.workload.clone(),
            record.concurrency,
        );
        groups.entry(key).or_default().push(record);
    }

    let _ = writeln!(out, "## Results\n");

    for ((fixture_id, workload, concurrency), group) in &groups {
        let _ = writeln!(out, "### {fixture_id} / {workload} (c={concurrency})\n");

        let _ = writeln!(
            out,
            "| Engine | Wall (ms) | Ops | Ops/sec | Retries | Aborts | Integrity | Error |"
        );
        let _ = writeln!(
            out,
            "|--------|-----------|-----|---------|---------|--------|-----------|-------|"
        );

        for record in group {
            let r = &record.report;
            let integrity = r.correctness.integrity_check_ok.map_or_else(
                || "-".to_owned(),
                |ok| {
                    if ok {
                        "ok".to_owned()
                    } else {
                        "FAIL".to_owned()
                    }
                },
            );
            let error = r.error.as_deref().unwrap_or("-");
            let error_display = if error.len() > 40 {
                format!("{}...", &error[..37])
            } else {
                error.to_owned()
            };

            let _ = writeln!(
                out,
                "| {} | {} | {} | {:.1} | {} | {} | {} | {} |",
                record.engine.name,
                r.wall_time_ms,
                r.ops_total,
                r.ops_per_sec,
                r.retries,
                r.aborts,
                integrity,
                error_display,
            );
        }

        let _ = writeln!(out);
    }

    out
}

// ── Markdown rendering from BenchmarkSummary ───────────────────────────

/// Render a list of [`BenchmarkSummary`] records into a Markdown summary.
///
/// Each summary gets its own section with latency and throughput tables.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn render_benchmark_summaries_markdown(summaries: &[BenchmarkSummary]) -> String {
    let mut out = String::with_capacity(4096);

    let _ = writeln!(out, "# Benchmark Report\n");

    if summaries.is_empty() {
        let _ = writeln!(out, "_No benchmarks to report._");
        return out;
    }

    // Methodology from first summary.
    let meth = &summaries[0].methodology;
    let _ = writeln!(out, "## Methodology\n");
    let _ = writeln!(out, "- **Version:** {}", meth.version);
    let _ = writeln!(out, "- **Primary statistic:** {}", meth.primary_statistic);
    let _ = writeln!(out, "- **Tail statistic:** {}\n", meth.tail_statistic);

    // Environment from first summary.
    let env = &summaries[0].environment;
    let _ = writeln!(out, "## Environment\n");
    let _ = writeln!(out, "- **OS:** {}", env.os);
    let _ = writeln!(out, "- **Arch:** {}", env.arch);
    let _ = writeln!(out, "- **CPUs:** {}", env.cpu_count);
    if let Some(ref model) = env.cpu_model {
        let _ = writeln!(out, "- **CPU model:** {model}");
    }
    if let Some(ram) = env.ram_bytes {
        let _ = writeln!(out, "- **RAM:** {:.1} GiB", ram as f64 / 1_073_741_824.0);
    }
    let _ = writeln!(out, "- **rustc:** {}", env.rustc_version);
    let _ = writeln!(out, "- **Profile:** {}\n", env.cargo_profile);

    // Summary table.
    let _ = writeln!(out, "## Summary\n");
    let _ = writeln!(
        out,
        "| Benchmark | Engine | Iters | Median (ms) | p95 (ms) | p99 (ms) | Stddev (ms) | Median Ops/s | Peak Ops/s |"
    );
    let _ = writeln!(
        out,
        "|-----------|--------|-------|-------------|----------|----------|-------------|--------------|------------|"
    );

    for s in summaries {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {:.0} |",
            s.benchmark_id,
            s.engine,
            s.measurement_count,
            s.latency.median_ms,
            s.latency.p95_ms,
            s.latency.p99_ms,
            s.latency.stddev_ms,
            s.throughput.median_ops_per_sec,
            s.throughput.peak_ops_per_sec,
        );
    }

    let _ = writeln!(out);

    // Detailed per-benchmark sections.
    for s in summaries {
        let _ = writeln!(out, "### {}\n", s.benchmark_id);
        let _ = writeln!(out, "- **Fixture:** {}", s.fixture_id);
        let _ = writeln!(out, "- **Workload:** {}", s.workload);
        let _ = writeln!(out, "- **Concurrency:** {}", s.concurrency);
        let _ = writeln!(out, "- **Warmup iterations:** {}", s.warmup_count);
        let _ = writeln!(out, "- **Measurement iterations:** {}", s.measurement_count);
        let _ = writeln!(
            out,
            "- **Total measurement time:** {} ms\n",
            s.total_measurement_ms
        );

        let _ = writeln!(out, "**Latency (ms):**\n");
        let _ = writeln!(out, "| Min | Max | Mean | Median | p95 | p99 | Stddev |");
        let _ = writeln!(out, "|-----|-----|------|--------|-----|-----|--------|");
        let _ = writeln!(
            out,
            "| {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} |\n",
            s.latency.min_ms,
            s.latency.max_ms,
            s.latency.mean_ms,
            s.latency.median_ms,
            s.latency.p95_ms,
            s.latency.p99_ms,
            s.latency.stddev_ms,
        );

        let _ = writeln!(out, "**Throughput (ops/sec):**\n");
        let _ = writeln!(out, "| Mean | Median | Peak |");
        let _ = writeln!(out, "|------|--------|------|");
        let _ = writeln!(
            out,
            "| {:.0} | {:.0} | {:.0} |\n",
            s.throughput.mean_ops_per_sec,
            s.throughput.median_ops_per_sec,
            s.throughput.peak_ops_per_sec,
        );

        // Per-iteration table (truncated if large).
        let max_rows = 20;
        let show_count = s.iterations.len().min(max_rows);
        if !s.iterations.is_empty() {
            let _ = writeln!(
                out,
                "<details>\n<summary>Iteration details ({} iterations, showing first {show_count})</summary>\n",
                s.iterations.len()
            );
            let _ = writeln!(
                out,
                "| # | Wall (ms) | Ops/sec | Ops | Retries | Aborts | Error |"
            );
            let _ = writeln!(
                out,
                "|---|-----------|---------|-----|---------|--------|-------|"
            );

            for iter in s.iterations.iter().take(max_rows) {
                let error = iter.error.as_deref().unwrap_or("-");
                let _ = writeln!(
                    out,
                    "| {} | {} | {:.1} | {} | {} | {} | {} |",
                    iter.iteration,
                    iter.wall_time_ms,
                    iter.ops_per_sec,
                    iter.ops_total,
                    iter.retries,
                    iter.aborts,
                    error,
                );
            }

            if s.iterations.len() > max_rows {
                let _ = writeln!(
                    out,
                    "\n_... {} more iterations not shown._",
                    s.iterations.len() - max_rows
                );
            }

            let _ = writeln!(out, "\n</details>\n");
        }
    }

    out
}

// ── Performance matrix report (bd-2als.2.3) ─────────────────────────────

/// Configuration for rendering a perf report.
#[derive(Debug, Clone)]
pub struct PerfReportConfig {
    /// Harness settings (PRAGMAs, journal mode, etc.) for the baseline section.
    pub settings: HarnessSettings,
    /// Seed used for deterministic workload generation.
    pub seed: u64,
    /// Scale factor.
    pub scale: u32,
}

/// Render a [`PerfResult`] into a human-readable Markdown performance report.
///
/// Produces:
/// - Baseline settings (PRAGMAs, seed, scale)
/// - Per-fixture / per-workload comparison tables
/// - Speedup ratios (fsqlite vs sqlite3 baseline)
/// - Concurrency scaling analysis
/// - Latency percentile comparisons
///
/// Bead: bd-2als.2.3
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn render_perf_report_markdown(result: &PerfResult, config: &PerfReportConfig) -> String {
    let mut out = String::with_capacity(8192);

    let _ = writeln!(out, "# Performance Report\n");

    if result.cells.is_empty() {
        let _ = writeln!(out, "_No benchmark cells to report._");
        return out;
    }

    // ── Baseline settings ──────────────────────────────────────────
    render_baseline_settings(&mut out, config);

    // ── Overall summary ────────────────────────────────────────────
    let _ = writeln!(out, "## Matrix Summary\n");
    let _ = writeln!(
        out,
        "- **Total cells:** {}\n- **Succeeded:** {}\n- **Errors:** {}\n",
        result.total_cells, result.success_count, result.error_count
    );

    // Collect successful cells grouped by (fixture, workload, concurrency).
    let mut groups: BTreeMap<FixtureWorkloadConcurrencyKey, Vec<&CellOutcome>> = BTreeMap::new();
    for cell in &result.cells {
        if cell.summary.is_some() {
            let key = (
                cell.fixture_id.clone(),
                cell.workload.clone(),
                cell.concurrency,
            );
            groups.entry(key).or_default().push(cell);
        }
    }

    // ── Speedup summary table ──────────────────────────────────────
    render_speedup_summary(&mut out, &groups);

    // ── Per-fixture detail sections ────────────────────────────────
    render_fixture_details(&mut out, &groups);

    // ── Concurrency scaling analysis ───────────────────────────────
    render_scaling_analysis(&mut out, &result.cells);

    // ── Error summary ──────────────────────────────────────────────
    let errors: Vec<_> = result.cells.iter().filter(|c| c.error.is_some()).collect();
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

/// Render the baseline settings section.
fn render_baseline_settings(out: &mut String, config: &PerfReportConfig) {
    let s = &config.settings;
    let _ = writeln!(out, "## Baseline Settings\n");
    let _ = writeln!(out, "| Parameter | Value |");
    let _ = writeln!(out, "|-----------|-------|");
    let _ = writeln!(out, "| Journal mode | {} |", s.journal_mode);
    let _ = writeln!(out, "| Synchronous | {} |", s.synchronous);
    let _ = writeln!(out, "| Cache size | {} |", s.cache_size);
    let _ = writeln!(out, "| Page size | {} |", s.page_size);
    let _ = writeln!(out, "| Busy timeout (ms) | {} |", s.busy_timeout_ms);
    let _ = writeln!(out, "| Seed | {} |", config.seed);
    let _ = writeln!(out, "| Scale | {} |", config.scale);
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "> **Note:** sqlite3 concurrency is serialized via busy-timeout/retry. \
         Reported sqlite3 \"concurrent\" results reflect retry-based serialization, \
         not genuine parallel writes.\n"
    );
}

/// Render the speedup summary table comparing fsqlite vs sqlite3.
fn render_speedup_summary(
    out: &mut String,
    groups: &BTreeMap<FixtureWorkloadConcurrencyKey, Vec<&CellOutcome>>,
) {
    // Collect pairs where both engines are present.
    let mut rows: Vec<SpeedupRow> = Vec::new();

    for ((fixture_id, workload, concurrency), cells) in groups {
        let sqlite3 = cells
            .iter()
            .find(|c| c.engine == "sqlite3")
            .and_then(|c| c.summary.as_ref());
        let fsqlite = cells
            .iter()
            .find(|c| c.engine == "fsqlite")
            .and_then(|c| c.summary.as_ref());

        if let (Some(sq), Some(fs)) = (sqlite3, fsqlite) {
            let speedup = if sq.latency.median_ms > 0.0 {
                sq.latency.median_ms / fs.latency.median_ms
            } else {
                0.0
            };
            rows.push(SpeedupRow {
                fixture_id: fixture_id.clone(),
                workload: workload.clone(),
                concurrency: *concurrency,
                sqlite3_median_ms: sq.latency.median_ms,
                fsqlite_median_ms: fs.latency.median_ms,
                sqlite3_p95_ms: sq.latency.p95_ms,
                fsqlite_p95_ms: fs.latency.p95_ms,
                speedup,
            });
        }
    }

    if rows.is_empty() {
        return;
    }

    let _ = writeln!(out, "## Speedup Summary (fsqlite vs sqlite3)\n");
    let _ = writeln!(
        out,
        "| Fixture | Workload | c | sqlite3 med (ms) | fsqlite med (ms) | Speedup | sqlite3 p95 | fsqlite p95 |"
    );
    let _ = writeln!(
        out,
        "|---------|----------|---|------------------|------------------|---------|-------------|-------------|"
    );

    for r in &rows {
        let speedup_str = if r.speedup >= 1.0 {
            format!("{:.2}x", r.speedup)
        } else if r.speedup > 0.0 {
            format!("{:.2}x (slower)", r.speedup)
        } else {
            "-".to_owned()
        };

        let _ = writeln!(
            out,
            "| {} | {} | {} | {:.1} | {:.1} | {} | {:.1} | {:.1} |",
            r.fixture_id,
            r.workload,
            r.concurrency,
            r.sqlite3_median_ms,
            r.fsqlite_median_ms,
            speedup_str,
            r.sqlite3_p95_ms,
            r.fsqlite_p95_ms,
        );
    }

    let _ = writeln!(out);
}

/// Internal row for the speedup table.
struct SpeedupRow {
    fixture_id: String,
    workload: String,
    concurrency: u16,
    sqlite3_median_ms: f64,
    fsqlite_median_ms: f64,
    sqlite3_p95_ms: f64,
    fsqlite_p95_ms: f64,
    speedup: f64,
}

/// Render per-fixture detail sections.
fn render_fixture_details(
    out: &mut String,
    groups: &BTreeMap<FixtureWorkloadConcurrencyKey, Vec<&CellOutcome>>,
) {
    // Group by fixture.
    let mut by_fixture: PerfByFixture<'_> = BTreeMap::new();
    for ((fixture_id, workload, concurrency), cells) in groups {
        by_fixture.entry(fixture_id.as_str()).or_default().push((
            workload.as_str(),
            *concurrency,
            cells.clone(),
        ));
    }

    if by_fixture.is_empty() {
        return;
    }

    let _ = writeln!(out, "## Per-Fixture Results\n");

    for (fixture_id, workloads) in &by_fixture {
        let _ = writeln!(out, "### {fixture_id}\n");

        let _ = writeln!(
            out,
            "| Engine | Workload | c | Median (ms) | p95 (ms) | p99 (ms) | Stddev (ms) | Median ops/s | Retries* |"
        );
        let _ = writeln!(
            out,
            "|--------|----------|---|-------------|----------|----------|-------------|--------------|----------|"
        );

        for (workload, concurrency, cells) in workloads {
            for cell in cells {
                if let Some(ref summary) = cell.summary {
                    let total_retries: u64 = summary.iterations.iter().map(|i| i.retries).sum();
                    let _ = writeln!(
                        out,
                        "| {} | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {} |",
                        cell.engine,
                        workload,
                        concurrency,
                        summary.latency.median_ms,
                        summary.latency.p95_ms,
                        summary.latency.p99_ms,
                        summary.latency.stddev_ms,
                        summary.throughput.median_ops_per_sec,
                        total_retries,
                    );
                }
            }
        }

        let _ = writeln!(
            out,
            "\n\\* _Retries = total busy/lock retries across all measurement iterations._\n"
        );
    }
}

/// Render concurrency scaling analysis with ASCII sparklines.
///
/// Groups results by (engine, fixture, workload) and shows how throughput
/// changes as concurrency increases.
fn render_scaling_analysis(out: &mut String, cells: &[CellOutcome]) {
    // Group by (engine, fixture, workload) → sorted by concurrency.
    type ScaleKey = (String, String, String);
    let mut scale_groups: BTreeMap<ScaleKey, Vec<(u16, f64, f64)>> = BTreeMap::new();

    for cell in cells {
        if let Some(ref summary) = cell.summary {
            let key = (
                cell.engine.clone(),
                cell.fixture_id.clone(),
                cell.workload.clone(),
            );
            scale_groups.entry(key).or_default().push((
                cell.concurrency,
                summary.throughput.median_ops_per_sec,
                summary.latency.median_ms,
            ));
        }
    }

    // Only show scaling if there are entries with multiple concurrency levels.
    let multi_level: Vec<_> = scale_groups
        .iter()
        .filter(|(_, points)| points.len() > 1)
        .collect();

    if multi_level.is_empty() {
        return;
    }

    let _ = writeln!(out, "## Concurrency Scaling\n");

    for (key, points) in &multi_level {
        let (engine, fixture_id, workload) = key;
        let _ = writeln!(out, "### {} / {} / {}\n", engine, fixture_id, workload);

        let mut sorted: Vec<(u16, f64, f64)> = (*points).clone();
        sorted.sort_by_key(|(c, _, _)| *c);

        let _ = writeln!(
            out,
            "| Concurrency | Median ops/s | Median latency (ms) | Sparkline |"
        );
        let _ = writeln!(
            out,
            "|-------------|-------------|---------------------|-----------|"
        );

        let max_ops = sorted
            .iter()
            .map(|(_, ops, _)| *ops)
            .fold(0.0_f64, f64::max);

        for &(c, ops, lat) in &sorted {
            let spark = ascii_bar(ops, max_ops, 20);
            let _ = writeln!(out, "| {} | {:.0} | {:.1} | {} |", c, ops, lat, spark);
        }

        // Compute scaling efficiency vs single-threaded baseline.
        if let Some(&(base_c, base_ops, _)) = sorted.first() {
            if base_ops > 0.0 && base_c == 1 {
                let _ = writeln!(out, "\n_Scaling efficiency (vs c=1):_\n");
                for &(c, ops, _) in &sorted {
                    if c > 1 {
                        #[allow(clippy::cast_precision_loss)]
                        let ideal = base_ops * f64::from(c);
                        let efficiency = ops / ideal * 100.0;
                        let _ = writeln!(
                            out,
                            "- c={c}: {:.0} ops/s ({:.0}% of linear scaling)",
                            ops, efficiency
                        );
                    }
                }
            }
        }

        let _ = writeln!(out);
    }
}

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
        bar.push('\u{2588}'); // full block
    }
    for _ in filled..width {
        bar.push('\u{2591}'); // light shade
    }
    bar
}

// ── File-based convenience functions ───────────────────────────────────

/// Read a JSONL file and render run records to Markdown.
///
/// # Errors
///
/// Returns an I/O error if the file cannot be read.
pub fn render_run_records_from_file(path: &Path) -> std::io::Result<String> {
    let content = std::fs::read_to_string(path)?;
    let records = parse_run_records_jsonl(&content);
    Ok(render_run_records_markdown(&records))
}

/// Read a JSONL file and render benchmark summaries to Markdown.
///
/// # Errors
///
/// Returns an I/O error if the file cannot be read.
pub fn render_benchmark_summaries_from_file(path: &Path) -> std::io::Result<String> {
    let content = std::fs::read_to_string(path)?;
    let summaries = parse_benchmark_summaries_jsonl(&content);
    Ok(render_benchmark_summaries_markdown(&summaries))
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark::{BenchmarkConfig, BenchmarkMeta, run_benchmark};
    use crate::methodology::EnvironmentMeta;
    use crate::perf_runner::PERF_RESULT_SCHEMA_V1;
    use crate::report::{CorrectnessReport, EngineInfo, EngineRunReport, RunRecordV1Args};

    fn dummy_engine_report(wall_ms: u64, ops: u64) -> EngineRunReport {
        #[allow(clippy::cast_precision_loss)]
        let ops_per_sec = if wall_ms > 0 {
            ops as f64 / (wall_ms as f64 / 1000.0)
        } else {
            0.0
        };
        EngineRunReport {
            wall_time_ms: wall_ms,
            ops_total: ops,
            ops_per_sec,
            retries: 2,
            aborts: 0,
            correctness: CorrectnessReport {
                raw_sha256_match: None,
                dump_match: None,
                canonical_sha256_match: None,
                integrity_check_ok: Some(true),
                raw_sha256: None,
                canonical_sha256: None,
                logical_sha256: None,
                notes: None,
            },
            latency_ms: None,
            error: None,
        }
    }

    fn make_run_record(engine: &str, fixture: &str, workload: &str, c: u16) -> RunRecordV1 {
        RunRecordV1::new(RunRecordV1Args {
            recorded_unix_ms: 1_700_000_000_000,
            environment: EnvironmentMeta::capture("test"),
            engine: EngineInfo {
                name: engine.to_owned(),
                sqlite_version: if engine == "sqlite3" {
                    Some("3.45.0".to_owned())
                } else {
                    None
                },
                fsqlite_git: None,
            },
            fixture_id: fixture.to_owned(),
            golden_path: None,
            golden_sha256: None,
            workload: workload.to_owned(),
            concurrency: c,
            ops_count: 100,
            report: dummy_engine_report(500, 100),
        })
    }

    #[test]
    fn parse_run_records_from_jsonl() {
        let r1 = make_run_record("sqlite3", "db-a", "inserts", 1);
        let r2 = make_run_record("fsqlite", "db-a", "inserts", 1);

        let line1 = serde_json::to_string(&r1).unwrap();
        let line2 = serde_json::to_string(&r2).unwrap();
        let jsonl = format!("{line1}\n{line2}\n\n");

        let parsed = parse_run_records_jsonl(&jsonl);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].engine.name, "sqlite3");
        assert_eq!(parsed[1].engine.name, "fsqlite");
    }

    #[test]
    fn render_run_records_empty() {
        let md = render_run_records_markdown(&[]);
        assert!(md.contains("No records to report"));
    }

    #[test]
    fn render_run_records_produces_table() {
        let records = vec![
            make_run_record("sqlite3", "db-a", "inserts", 4),
            make_run_record("fsqlite", "db-a", "inserts", 4),
        ];

        let md = render_run_records_markdown(&records);
        assert!(md.contains("# E2E Run Report"));
        assert!(md.contains("## Methodology"));
        assert!(md.contains("## Results"));
        assert!(md.contains("db-a / inserts (c=4)"));
        assert!(md.contains("| sqlite3 |"));
        assert!(md.contains("| fsqlite |"));
        assert!(md.contains("| ok |"));
    }

    #[test]
    fn render_run_records_groups_by_fixture_workload_concurrency() {
        let records = vec![
            make_run_record("sqlite3", "db-a", "inserts", 1),
            make_run_record("fsqlite", "db-a", "inserts", 1),
            make_run_record("sqlite3", "db-b", "updates", 4),
            make_run_record("fsqlite", "db-b", "updates", 4),
        ];

        let md = render_run_records_markdown(&records);
        assert!(md.contains("db-a / inserts (c=1)"));
        assert!(md.contains("db-b / updates (c=4)"));
    }

    #[test]
    fn render_benchmark_summaries_empty() {
        let md = render_benchmark_summaries_markdown(&[]);
        assert!(md.contains("No benchmarks to report"));
    }

    #[test]
    fn render_benchmark_summaries_produces_tables() {
        let config = BenchmarkConfig {
            warmup_iterations: 1,
            min_iterations: 3,
            measurement_time_secs: 0,
        };
        let meta = BenchmarkMeta {
            engine: "sqlite3".to_owned(),
            workload: "inserts".to_owned(),
            fixture_id: "db-a".to_owned(),
            concurrency: 4,
            cargo_profile: "test".to_owned(),
        };

        let summary = run_benchmark(&config, &meta, |_| {
            Ok::<_, String>(dummy_engine_report(100, 1000))
        });

        let md = render_benchmark_summaries_markdown(&[summary]);
        assert!(md.contains("# Benchmark Report"));
        assert!(md.contains("## Methodology"));
        assert!(md.contains("## Environment"));
        assert!(md.contains("## Summary"));
        assert!(md.contains("sqlite3:inserts:db-a:c4"));
        assert!(md.contains("Latency (ms)"));
        assert!(md.contains("Throughput (ops/sec)"));
        assert!(md.contains("Iteration details"));
    }

    #[test]
    fn parse_benchmark_summaries_from_jsonl() {
        let config = BenchmarkConfig {
            warmup_iterations: 1,
            min_iterations: 2,
            measurement_time_secs: 0,
        };
        let meta = BenchmarkMeta {
            engine: "test".to_owned(),
            workload: "w".to_owned(),
            fixture_id: "f".to_owned(),
            concurrency: 1,
            cargo_profile: "test".to_owned(),
        };

        let summary = run_benchmark(&config, &meta, |_| {
            Ok::<_, String>(dummy_engine_report(50, 500))
        });

        let line = summary.to_jsonl().unwrap();
        let jsonl = format!("{line}\n");

        let parsed = parse_benchmark_summaries_jsonl(&jsonl);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].benchmark_id, "test:w:f:c1");
    }

    #[test]
    fn render_benchmark_summary_truncates_long_iteration_list() {
        let config = BenchmarkConfig {
            warmup_iterations: 0,
            min_iterations: 30,
            measurement_time_secs: 0,
        };
        let meta = BenchmarkMeta {
            engine: "test".to_owned(),
            workload: "w".to_owned(),
            fixture_id: "f".to_owned(),
            concurrency: 1,
            cargo_profile: "test".to_owned(),
        };

        let summary = run_benchmark(&config, &meta, |_| {
            Ok::<_, String>(dummy_engine_report(10, 100))
        });

        let md = render_benchmark_summaries_markdown(&[summary]);
        assert!(md.contains("30 iterations, showing first 20"));
        assert!(md.contains("10 more iterations not shown"));
    }

    #[test]
    fn error_in_run_record_shown_in_markdown() {
        let mut record = make_run_record("sqlite3", "db-err", "inserts", 1);
        record.report.error = Some("database locked".to_owned());
        record.report.correctness.integrity_check_ok = Some(false);

        let md = render_run_records_markdown(&[record]);
        assert!(md.contains("database locked"));
        assert!(md.contains("FAIL"));
    }

    #[test]
    fn long_error_message_truncated() {
        let mut record = make_run_record("sqlite3", "db-err", "inserts", 1);
        record.report.error = Some("a".repeat(100));

        let md = render_run_records_markdown(&[record]);
        // Error should be truncated to ~40 chars
        assert!(md.contains("..."));
    }

    // ── Perf report tests (bd-2als.2.3) ───────────────────────────────

    fn make_benchmark_summary(
        engine: &str,
        fixture: &str,
        workload: &str,
        c: u16,
    ) -> BenchmarkSummary {
        let bench_config = BenchmarkConfig {
            warmup_iterations: 1,
            min_iterations: 3,
            measurement_time_secs: 0,
        };
        let meta = BenchmarkMeta {
            engine: engine.to_owned(),
            workload: workload.to_owned(),
            fixture_id: fixture.to_owned(),
            concurrency: c,
            cargo_profile: "test".to_owned(),
        };

        // Use different wall times for different engines to produce distinct stats.
        let wall_ms: u64 = if engine == "sqlite3" { 200 } else { 100 };
        run_benchmark(&bench_config, &meta, |_| {
            Ok::<_, String>(dummy_engine_report(wall_ms, 1000))
        })
    }

    fn make_cell_outcome(engine: &str, fixture: &str, workload: &str, c: u16) -> CellOutcome {
        CellOutcome {
            summary: Some(make_benchmark_summary(engine, fixture, workload, c)),
            error: None,
            engine: engine.to_owned(),
            fixture_id: fixture.to_owned(),
            workload: workload.to_owned(),
            concurrency: c,
        }
    }

    fn default_perf_report_config() -> PerfReportConfig {
        PerfReportConfig {
            settings: HarnessSettings::default(),
            seed: 42,
            scale: 100,
        }
    }

    #[test]
    fn perf_report_empty() {
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 0,
            success_count: 0,
            error_count: 0,
            cells: Vec::new(),
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());
        assert!(md.contains("No benchmark cells to report"));
    }

    #[test]
    fn perf_report_baseline_settings() {
        let cells = vec![make_cell_outcome("sqlite3", "db-a", "inserts", 1)];
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 1,
            success_count: 1,
            error_count: 0,
            cells,
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());
        assert!(
            md.contains("## Baseline Settings"),
            "missing baseline settings section"
        );
        assert!(md.contains("| Journal mode | wal |"));
        assert!(md.contains("| Synchronous | NORMAL |"));
        assert!(md.contains("| Cache size | -2000 |"));
        assert!(md.contains("| Seed | 42 |"));
        assert!(md.contains("| Scale | 100 |"));
        assert!(md.contains("busy-timeout/retry"));
    }

    #[test]
    fn perf_report_speedup_summary() {
        let cells = vec![
            make_cell_outcome("sqlite3", "db-a", "inserts", 1),
            make_cell_outcome("fsqlite", "db-a", "inserts", 1),
        ];
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 2,
            success_count: 2,
            error_count: 0,
            cells,
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());
        assert!(md.contains("## Speedup Summary"), "missing speedup section");
        assert!(md.contains("sqlite3 med (ms)"), "missing sqlite3 column");
        assert!(md.contains("fsqlite med (ms)"), "missing fsqlite column");
    }

    #[test]
    fn perf_report_per_fixture_details() {
        let cells = vec![
            make_cell_outcome("sqlite3", "db-a", "inserts", 1),
            make_cell_outcome("fsqlite", "db-a", "inserts", 1),
            make_cell_outcome("sqlite3", "db-b", "updates", 4),
            make_cell_outcome("fsqlite", "db-b", "updates", 4),
        ];
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 4,
            success_count: 4,
            error_count: 0,
            cells,
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());
        assert!(
            md.contains("## Per-Fixture Results"),
            "missing per-fixture section"
        );
        assert!(md.contains("### db-a"), "missing db-a fixture heading");
        assert!(md.contains("### db-b"), "missing db-b fixture heading");
    }

    #[test]
    fn perf_report_scaling_analysis() {
        let cells = vec![
            make_cell_outcome("fsqlite", "db-a", "inserts", 1),
            make_cell_outcome("fsqlite", "db-a", "inserts", 4),
        ];
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 2,
            success_count: 2,
            error_count: 0,
            cells,
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());
        assert!(
            md.contains("## Concurrency Scaling"),
            "missing scaling section"
        );
        assert!(md.contains("Sparkline"), "missing sparkline column");
        assert!(
            md.contains("Scaling efficiency"),
            "missing scaling efficiency"
        );
    }

    #[test]
    fn perf_report_errors_section() {
        let cells = vec![CellOutcome {
            summary: None,
            error: Some("unknown preset: bogus".to_owned()),
            engine: "sqlite3".to_owned(),
            fixture_id: "db-a".to_owned(),
            workload: "bogus".to_owned(),
            concurrency: 1,
        }];
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 1,
            success_count: 0,
            error_count: 1,
            cells,
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());
        assert!(md.contains("## Errors"), "missing errors section");
        assert!(
            md.contains("unknown preset: bogus"),
            "missing error message"
        );
    }

    #[test]
    fn ascii_bar_rendering() {
        let bar = ascii_bar(50.0, 100.0, 10);
        assert_eq!(bar.chars().count(), 10);

        let full = ascii_bar(100.0, 100.0, 10);
        assert!(
            full.contains('\u{2588}'),
            "full bar should contain block chars"
        );

        let empty = ascii_bar(0.0, 100.0, 10);
        assert!(
            empty.contains('\u{2591}'),
            "empty bar should contain light shade"
        );

        let zero_max = ascii_bar(50.0, 0.0, 10);
        assert_eq!(zero_max, " ".repeat(10));
    }

    #[test]
    fn perf_report_cell_outcome_roundtrip() {
        let cell = make_cell_outcome("sqlite3", "db-a", "inserts", 1);
        let json = serde_json::to_string(&cell).unwrap();
        let parsed: CellOutcome = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.engine, "sqlite3");
        assert_eq!(parsed.fixture_id, "db-a");
        assert!(parsed.summary.is_some());

        // Test JSONL parsing convenience.
        let jsonl = format!("{json}\n");
        let cells = parse_perf_result_jsonl(&jsonl);
        assert_eq!(cells.len(), 1);
    }

    #[test]
    fn perf_report_no_scaling_with_single_concurrency() {
        let cells = vec![
            make_cell_outcome("sqlite3", "db-a", "inserts", 1),
            make_cell_outcome("fsqlite", "db-a", "inserts", 1),
        ];
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 2,
            success_count: 2,
            error_count: 0,
            cells,
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());
        // Should NOT show concurrency scaling when there's only one concurrency level.
        assert!(!md.contains("## Concurrency Scaling"));
    }
}
