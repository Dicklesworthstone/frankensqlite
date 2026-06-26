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

use serde::de::DeserializeOwned;

use crate::HarnessSettings;
use crate::benchmark::{
    BenchmarkCausalChainLink, BenchmarkCausalScorecard, BenchmarkSummary,
    build_benchmark_causal_scorecard_report,
};
use crate::overlay_honesty_gate::{
    BenchmarkHonestGateClassification, build_benchmark_honest_gate_report,
};
use crate::perf_runner::{CellOutcome, PerfResult};
use crate::report::RunRecordV1;

type FixtureWorkloadConcurrencyKey = (String, String, u16);
type PerfByFixture<'a> = BTreeMap<&'a str, Vec<(&'a str, u16, Vec<&'a CellOutcome>)>>;

fn benchmark_mode_label(summary: &BenchmarkSummary) -> &str {
    summary.comparison_mode_id()
}

fn cell_mode_label(cell: &CellOutcome) -> &str {
    cell.mode_id
        .as_deref()
        .or_else(|| {
            cell.summary
                .as_ref()
                .map(crate::benchmark::BenchmarkSummary::comparison_mode_id)
        })
        .unwrap_or(cell.engine.as_str())
}

fn mode_sort_key(mode: &str) -> (u8, &str) {
    match mode {
        "sqlite_reference" | "sqlite3" => (0, mode),
        "fsqlite_mvcc" => (1, mode),
        "fsqlite_single_writer" => (2, mode),
        _ => (3, mode),
    }
}

fn mode_display_name(mode: &str) -> &str {
    match mode {
        "sqlite_reference" | "sqlite3" => "sqlite_reference",
        "fsqlite_mvcc" => "fsqlite_mvcc",
        "fsqlite_single_writer" => "fsqlite_single_writer",
        _ => mode,
    }
}

fn ratio_string(numerator: Option<f64>, denominator: Option<f64>) -> String {
    match (numerator, denominator) {
        (Some(num), Some(den)) if den > 0.0 && num.is_finite() && den.is_finite() => {
            format!("{:.2}x", num / den)
        }
        _ => "-".to_owned(),
    }
}

fn format_ratio_value(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_owned(), |ratio| format!("{ratio:.2}x"))
}

fn scorecard_mode_label(scorecard: &BenchmarkCausalScorecard) -> &str {
    scorecard.row_identity.mode_id.as_str()
}

fn format_scorecard_wall_delta(delta_ms: f64) -> String {
    if delta_ms.abs() <= f64::EPSILON {
        "neutral".to_owned()
    } else if delta_ms > 0.0 {
        format!("{delta_ms:.1} saved")
    } else {
        format!("{:.1} slower", delta_ms.abs())
    }
}

fn format_scorecard_share(share_basis_points: Option<u32>) -> String {
    share_basis_points
        .map(|bps| format!("{:.1}%", f64::from(bps) / 100.0))
        .unwrap_or_else(|| "-".to_owned())
}

fn summarize_scorecard_counter_deltas(link: &BenchmarkCausalChainLink) -> String {
    link.counter_deltas
        .iter()
        .take(4)
        .map(|delta| format!("`{}` {}", delta.counter_id, delta.summary))
        .collect::<Vec<_>>()
        .join("; ")
}

// ── Parsing ────────────────────────────────────────────────────────────

/// Parse diagnostic for a malformed JSONL line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonlParseError {
    pub line: usize,
    pub error: String,
}

/// Parsed JSONL records plus malformed-line diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonlParseReport<T> {
    pub records: Vec<T>,
    pub invalid_lines: Vec<JsonlParseError>,
}

fn parse_jsonl_with_report<T: DeserializeOwned>(jsonl: &str) -> JsonlParseReport<T> {
    let mut records = Vec::new();
    let mut invalid_lines = Vec::new();

    for (idx, line) in jsonl.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<T>(trimmed) {
            Ok(record) => records.push(record),
            Err(err) => invalid_lines.push(JsonlParseError {
                line: idx + 1,
                error: err.to_string(),
            }),
        }
    }

    JsonlParseReport {
        records,
        invalid_lines,
    }
}

/// Parse a JSONL string into a list of [`RunRecordV1`] records.
///
/// Blank lines are skipped; malformed lines are ignored.
#[must_use]
pub fn parse_run_records_jsonl(jsonl: &str) -> Vec<RunRecordV1> {
    parse_run_records_jsonl_report(jsonl).records
}

/// Parse JSONL into [`RunRecordV1`] records with malformed-line diagnostics.
#[must_use]
pub fn parse_run_records_jsonl_report(jsonl: &str) -> JsonlParseReport<RunRecordV1> {
    parse_jsonl_with_report(jsonl)
}

/// Parse a JSONL string into a list of [`BenchmarkSummary`] records.
///
/// Blank lines are skipped; malformed lines are ignored.
#[must_use]
pub fn parse_benchmark_summaries_jsonl(jsonl: &str) -> Vec<BenchmarkSummary> {
    parse_benchmark_summaries_jsonl_report(jsonl).records
}

/// Parse JSONL into [`BenchmarkSummary`] records with malformed-line diagnostics.
#[must_use]
pub fn parse_benchmark_summaries_jsonl_report(jsonl: &str) -> JsonlParseReport<BenchmarkSummary> {
    parse_jsonl_with_report(jsonl)
}

/// Parse a JSONL string of [`CellOutcome`] records.
///
/// Blank lines are skipped; malformed lines are ignored.
#[must_use]
pub fn parse_perf_result_jsonl(jsonl: &str) -> Vec<CellOutcome> {
    parse_perf_result_jsonl_report(jsonl).records
}

/// Parse JSONL into [`CellOutcome`] records with malformed-line diagnostics.
#[must_use]
pub fn parse_perf_result_jsonl_report(jsonl: &str) -> JsonlParseReport<CellOutcome> {
    parse_jsonl_with_report(jsonl)
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

    let _ = writeln!(out, "## Mandatory Perf Checklist\n");
    let _ = writeln!(
        out,
        "- [ ] Confirm every compared row uses the declared methodology version `{}` and that any build-profile or placement differences are disclosed before calling rows directly comparable.",
        meth.version
    );
    let _ = writeln!(
        out,
        "- [ ] Confirm the published bundle includes `summary.md`, `results.jsonl`, `scorecards.json`, and the row-level manifests before citing a wall-time or throughput claim."
    );
    let _ = writeln!(
        out,
        "- [ ] Confirm each optimization claim names the comparator row and causal-chain step that supports it; if the bridge row is missing, keep the negative finding in the report."
    );
    let _ = writeln!(
        out,
        "- [ ] Confirm retry policy, placement, and row identity are recorded for each comparison group so counter deltas can be matched up mechanically."
    );
    let _ = writeln!(
        out,
        "- [ ] Confirm regressions or advisory-only rows remain visible in the final report instead of being filtered out of the scorecard narrative."
    );
    let _ = writeln!(out);

    let mut comparison_groups: BTreeMap<FixtureWorkloadConcurrencyKey, Vec<&BenchmarkSummary>> =
        BTreeMap::new();
    for summary in summaries {
        let key = (
            summary.fixture_id.clone(),
            summary.workload.clone(),
            summary.concurrency,
        );
        comparison_groups.entry(key).or_default().push(summary);
    }

    let comparison_sections = comparison_groups
        .iter()
        .filter(|(_, group)| group.len() > 1)
        .collect::<Vec<_>>();
    if let Some(honest_gate) = build_benchmark_honest_gate_report(summaries) {
        let _ = writeln!(out, "## Critical Benchmark Gates\n");
        let _ = writeln!(
            out,
            "- **Verdict:** {}",
            honest_gate.honest_gate_summary.verdict
        );
        let _ = writeln!(
            out,
            "- **Healthy margin threshold:** {:.2}x throughput vs same-row sqlite_reference",
            honest_gate.healthy_margin_min
        );
        let _ = writeln!(
            out,
            "- **Aggregate rows are secondary:** {}",
            honest_gate.aggregate_rows_are_secondary
        );
        let _ = writeln!(
            out,
            "- **Critical surfaces are primary:** {}\n",
            honest_gate.critical_surface_primary
        );
        let _ = writeln!(
            out,
            "| Surface | Verdict | Expected | Present | Comparable | Red | Margin | Missing Baseline |"
        );
        let _ = writeln!(
            out,
            "|---------|---------|----------|---------|------------|-----|--------|------------------|"
        );
        for surface in &honest_gate.surfaces {
            let red_count = surface.below_parity_count + surface.tail_slower_than_sqlite_count;
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} | {} | {} |",
                surface.surface_id,
                surface.verdict,
                surface.expected_critical_row_count,
                surface.critical_row_count,
                surface.comparable_row_count,
                red_count,
                surface.parity_to_margin_count,
                surface.missing_baseline_count,
            );
        }
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "| Surface | Row | Throughput vs sqlite | Median Latency vs sqlite | p95 vs sqlite | p99 vs sqlite | Verdict | Retries | Aborts |"
        );
        let _ = writeln!(
            out,
            "|---------|-----|----------------------|---------------------------|---------------|---------------|---------|---------|--------|"
        );
        for row in &honest_gate.rows {
            let row_label = row
                .cell_key
                .row_id
                .clone()
                .unwrap_or_else(|| row.cell_key.benchmark_id.clone());
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                row.surface_id,
                row_label,
                format_ratio_value(row.throughput_ratio_vs_sqlite),
                format_ratio_value(row.median_latency_ratio_vs_sqlite),
                format_ratio_value(row.p95_latency_ratio_vs_sqlite),
                format_ratio_value(row.p99_latency_ratio_vs_sqlite),
                row.classification,
                row.retries_total,
                row.aborts_total,
            );
        }
        let _ = writeln!(out);
        if honest_gate.rows.iter().any(|row| {
            matches!(
                row.classification,
                BenchmarkHonestGateClassification::TailSlowerThanSqlite
            )
        }) {
            let _ = writeln!(
                out,
                "- Persistent high-thread rows stay red when throughput clears parity but p95/p99 remain slower than same-row sqlite_reference.\n"
            );
        }
    }
    if !comparison_sections.is_empty() {
        let _ = writeln!(out, "## Direct Comparison\n");
        for ((fixture_id, workload, concurrency), group) in comparison_sections {
            let _ = writeln!(out, "### {fixture_id} / {workload} (c={concurrency})\n");
            let _ = writeln!(
                out,
                "| Mode | Median (ms) | p95 (ms) | Median ops/s | Retries | Aborts | Retry Policy | Placement | Row |"
            );
            let _ = writeln!(
                out,
                "|------|-------------|----------|--------------|---------|--------|--------------|-----------|-----|"
            );
            let mut ordered = group.clone();
            ordered.sort_by(|left, right| {
                mode_sort_key(benchmark_mode_label(left))
                    .cmp(&mode_sort_key(benchmark_mode_label(right)))
            });
            for summary in ordered {
                let comparison = summary.comparison.as_ref();
                let retry_policy = comparison
                    .and_then(|meta| meta.provenance.retry_policy_id.as_deref())
                    .unwrap_or("-");
                let placement = comparison
                    .and_then(|meta| meta.row_identity.placement_profile_id.as_deref())
                    .unwrap_or("-");
                let row_id = comparison
                    .and_then(|meta| meta.row_identity.row_id.as_deref())
                    .unwrap_or("-");
                let _ = writeln!(
                    out,
                    "| {} | {:.1} | {:.1} | {:.0} | {} | {} | {} | {} | {} |",
                    mode_display_name(benchmark_mode_label(summary)),
                    summary.latency.median_ms,
                    summary.latency.p95_ms,
                    summary.throughput.median_ops_per_sec,
                    summary.total_iteration_retries(),
                    summary.total_iteration_aborts(),
                    retry_policy,
                    placement,
                    row_id,
                );
            }
            let _ = writeln!(out);
        }
    }

    let scorecard_report = build_benchmark_causal_scorecard_report(summaries);
    if !scorecard_report.groups.is_empty() {
        let _ = writeln!(out, "## Causal Scorecards\n");
        for group in &scorecard_report.groups {
            let _ = writeln!(
                out,
                "### {} / {} (c={})\n",
                group.fixture_id, group.workload, group.concurrency
            );
            for scorecard in &group.scorecards {
                let _ = writeln!(
                    out,
                    "#### {}\n",
                    mode_display_name(scorecard_mode_label(scorecard))
                );
                if let Some(ref row_id) = scorecard.row_identity.row_id {
                    let _ = writeln!(out, "- **Row:** {row_id}");
                }
                let _ = writeln!(
                    out,
                    "- **Baseline comparator:** {}",
                    scorecard.baseline_comparator
                );
                let _ = writeln!(out, "- **Claim:** {}", scorecard.claim_summary);
                let _ = writeln!(
                    out,
                    "- **Interpretation:** {}\n",
                    scorecard.interpretation_note
                );
                if scorecard.causal_chain.is_empty() {
                    let _ = writeln!(
                        out,
                        "_No attributable causal chain is available for this row._\n"
                    );
                } else {
                    let _ = writeln!(
                        out,
                        "| Step | Transition | Family | Wall-Time Delta (ms) | Share of Total Gain |"
                    );
                    let _ = writeln!(
                        out,
                        "|------|------------|--------|----------------------|---------------------|"
                    );
                    for link in &scorecard.causal_chain {
                        let _ = writeln!(
                            out,
                            "| {} | {} → {} | {} | {} | {} |",
                            link.rank,
                            mode_display_name(link.from_mode_id.as_str()),
                            mode_display_name(link.to_mode_id.as_str()),
                            link.optimization_family,
                            format_scorecard_wall_delta(link.attributed_wall_time_delta_ms),
                            format_scorecard_share(link.share_of_total_wall_time_gain_basis_points),
                        );
                    }
                    let _ = writeln!(out);
                    for link in &scorecard.causal_chain {
                        let _ = writeln!(out, "- **Step {}:** {}", link.rank, link.claim_summary);
                        let _ = writeln!(out, "- **Rationale:** {}", link.rationale);
                        let delta_summary = summarize_scorecard_counter_deltas(link);
                        if !delta_summary.is_empty() {
                            let _ = writeln!(out, "- **Counter deltas:** {delta_summary}");
                        }
                        if !link.evidence.is_empty() {
                            let _ = writeln!(out, "- **Evidence:** {}", link.evidence.join("; "));
                        }
                    }
                    let _ = writeln!(out);
                }
                if !scorecard.negative_findings.is_empty() {
                    let _ = writeln!(out, "**Negative Findings:**");
                    for finding in &scorecard.negative_findings {
                        let _ = writeln!(out, "- {finding}");
                    }
                    let _ = writeln!(out);
                }
            }
        }
    }

    // Summary table.
    let _ = writeln!(out, "## Summary\n");
    let _ = writeln!(
        out,
        "| Benchmark | Mode | Iters | Median (ms) | p95 (ms) | p99 (ms) | Stddev (ms) | Median Ops/s | Peak Ops/s |"
    );
    let _ = writeln!(
        out,
        "|-----------|------|-------|-------------|----------|----------|-------------|--------------|------------|"
    );

    for s in summaries {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {:.0} |",
            s.benchmark_id,
            mode_display_name(benchmark_mode_label(s)),
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
        let _ = writeln!(
            out,
            "- **Mode:** {}",
            mode_display_name(benchmark_mode_label(s))
        );
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
        if let Some(ref comparison) = s.comparison {
            if let Some(ref row_id) = comparison.row_identity.row_id {
                let _ = writeln!(out, "- **Canonical row:** {row_id}");
            }
            let _ = writeln!(
                out,
                "- **Build profile:** {}",
                comparison.row_identity.build_profile_id.as_str()
            );
            if let Some(ref retry_policy_id) = comparison.provenance.retry_policy_id {
                let _ = writeln!(out, "- **Retry policy:** {retry_policy_id}");
            }
            if let Some(ref placement_profile_id) = comparison.row_identity.placement_profile_id {
                let _ = writeln!(out, "- **Placement profile:** {placement_profile_id}");
            }
            if let Some(ref run_id) = comparison.row_identity.run_id {
                let _ = writeln!(out, "- **Run ID:** {run_id}");
            }
            if let Some(ref source_revision) = comparison.row_identity.source_revision {
                let _ = writeln!(out, "- **Source revision:** {source_revision}");
            }
            if let Some(ref hardware_class_id) = comparison.provenance.hardware_class_id {
                let _ = writeln!(out, "- **Hardware class:** {hardware_class_id}");
            }
            if let Some(layout) = comparison.artifact_layout.as_ref() {
                let _ = writeln!(
                    out,
                    "- **Artifact manifest:** {}",
                    layout.artifact_manifest_path.as_str()
                );
            }
            let _ = writeln!(out);
        }

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

    // Collect only fully comparable cells grouped by
    // (fixture, workload, concurrency).
    let mut groups: BTreeMap<FixtureWorkloadConcurrencyKey, Vec<&CellOutcome>> = BTreeMap::new();
    for cell in &result.cells {
        if cell.is_fully_comparable() {
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
                mode_display_name(cell_mode_label(cell)),
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
    let _ = writeln!(
        out,
        "| Busy timeout (ms) | {} |",
        HarnessSettings::benchmark_busy_timeout_ms()
    );
    let _ = writeln!(out, "| Seed | {} |", config.seed);
    let _ = writeln!(out, "| Scale | {} |", config.scale);
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "> **Note:** benchmark runs force `PRAGMA busy_timeout=0` for both engines so \
         contention shows up in the harness retry counters. sqlite3 still serializes \
         writes under contention; the report reflects retry-based serialization, not \
         genuine parallel writes.\n"
    );
}

/// Render the summary table comparing SQLite, MVCC, and forced single-writer.
fn render_speedup_summary(
    out: &mut String,
    groups: &BTreeMap<FixtureWorkloadConcurrencyKey, Vec<&CellOutcome>>,
) {
    let mut rows: Vec<ModeComparisonRow> = Vec::new();

    for ((fixture_id, workload, concurrency), cells) in groups {
        let sqlite = cells
            .iter()
            .find(|cell| matches!(cell_mode_label(cell), "sqlite_reference" | "sqlite3"))
            .and_then(|cell| cell.comparable_summary());
        let mvcc = cells
            .iter()
            .find(|cell| cell_mode_label(cell) == "fsqlite_mvcc")
            .and_then(|cell| cell.comparable_summary());
        let single_writer = cells
            .iter()
            .find(|cell| cell_mode_label(cell) == "fsqlite_single_writer")
            .and_then(|cell| cell.comparable_summary());

        if sqlite.is_some() || mvcc.is_some() || single_writer.is_some() {
            rows.push(ModeComparisonRow {
                fixture_id: fixture_id.clone(),
                workload: workload.clone(),
                concurrency: *concurrency,
                sqlite_median_ms: sqlite.map(|summary| summary.latency.median_ms),
                mvcc_median_ms: mvcc.map(|summary| summary.latency.median_ms),
                single_writer_median_ms: single_writer.map(|summary| summary.latency.median_ms),
                sqlite_p95_ms: sqlite.map(|summary| summary.latency.p95_ms),
                mvcc_p95_ms: mvcc.map(|summary| summary.latency.p95_ms),
                single_writer_p95_ms: single_writer.map(|summary| summary.latency.p95_ms),
            });
        }
    }

    if rows.is_empty() {
        return;
    }

    let _ = writeln!(out, "## Mode Comparison Summary\n");
    let _ = writeln!(
        out,
        "| Fixture | Workload | c | sqlite med (ms) | mvcc med (ms) | single-writer med (ms) | sqlite/mvcc | sqlite/single | single/mvcc | sqlite p95 | mvcc p95 | single p95 |"
    );
    let _ = writeln!(
        out,
        "|---------|----------|---|-----------------|---------------|---------------------|-------------|---------------|-------------|------------|----------|------------|"
    );

    for r in &rows {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            r.fixture_id,
            r.workload,
            r.concurrency,
            r.sqlite_median_ms
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.1}")),
            r.mvcc_median_ms
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.1}")),
            r.single_writer_median_ms
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.1}")),
            ratio_string(r.sqlite_median_ms, r.mvcc_median_ms),
            ratio_string(r.sqlite_median_ms, r.single_writer_median_ms),
            ratio_string(r.single_writer_median_ms, r.mvcc_median_ms),
            r.sqlite_p95_ms
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.1}")),
            r.mvcc_p95_ms
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.1}")),
            r.single_writer_p95_ms
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.1}")),
        );
    }

    let _ = writeln!(out);
}

/// Internal row for the speedup table.
struct ModeComparisonRow {
    fixture_id: String,
    workload: String,
    concurrency: u16,
    sqlite_median_ms: Option<f64>,
    mvcc_median_ms: Option<f64>,
    single_writer_median_ms: Option<f64>,
    sqlite_p95_ms: Option<f64>,
    mvcc_p95_ms: Option<f64>,
    single_writer_p95_ms: Option<f64>,
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
            "| Mode | Workload | c | Median (ms) | p95 (ms) | p99 (ms) | Stddev (ms) | Median ops/s | Retries* |"
        );
        let _ = writeln!(
            out,
            "|------|----------|---|-------------|----------|----------|-------------|--------------|----------|"
        );

        for (workload, concurrency, cells) in workloads {
            for cell in cells {
                if let Some(ref summary) = cell.summary {
                    let _ = writeln!(
                        out,
                        "| {} | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {} |",
                        mode_display_name(cell_mode_label(cell)),
                        workload,
                        concurrency,
                        summary.latency.median_ms,
                        summary.latency.p95_ms,
                        summary.latency.p99_ms,
                        summary.latency.stddev_ms,
                        summary.throughput.median_ops_per_sec,
                        summary.total_iteration_retries(),
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
        if let Some(summary) = cell.comparable_summary() {
            let key = (
                mode_display_name(cell_mode_label(cell)).to_owned(),
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
    use crate::benchmark::{
        BenchmarkComparisonMetadata, BenchmarkConfig, BenchmarkMeta, BenchmarkSummary,
        run_benchmark,
    };
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
            first_failure_diagnostic: None,
            storage_wiring: None,
            runtime_phase_timing: None,
            hot_path_profile: None,
        }
    }

    fn make_run_record(engine: &str, fixture: &str, workload: &str, c: u16) -> RunRecordV1 {
        RunRecordV1::new(RunRecordV1Args {
            recorded_unix_ms: 1_700_000_000_000,
            environment: EnvironmentMeta::capture("test"),
            engine: EngineInfo {
                name: engine.to_owned(),
                sqlite_version: if engine == "sqlite3" {
                    Some(fsqlite_types::FRANKENSQLITE_SQLITE_VERSION.to_owned())
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

    fn scorecard_summary(mode_id: &str, wall_ms: u64) -> BenchmarkSummary {
        let engine = match mode_id {
            "sqlite_reference" => "sqlite3",
            _ => "fsqlite",
        };
        let mut summary = run_benchmark(
            &BenchmarkConfig {
                warmup_iterations: 0,
                min_iterations: 1,
                measurement_time_secs: 0,
            },
            &BenchmarkMeta {
                engine: engine.to_owned(),
                workload: "mixed_read_write".to_owned(),
                fixture_id: "db-a".to_owned(),
                concurrency: 4,
                cargo_profile: "test".to_owned(),
            },
            |_| Ok::<_, String>(dummy_engine_report(wall_ms, 1_000)),
        );
        summary.comparison = Some(BenchmarkComparisonMetadata::anonymous(&summary, mode_id));
        summary
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
    fn parse_run_records_report_surfaces_invalid_lines() {
        let r1 = make_run_record("sqlite3", "db-a", "inserts", 1);
        let line1 = serde_json::to_string(&r1).unwrap();
        let jsonl = format!("{line1}\n{{bad json}}\n");

        let parsed = parse_run_records_jsonl_report(&jsonl);
        assert_eq!(parsed.records.len(), 1);
        assert_eq!(parsed.invalid_lines.len(), 1);
        assert_eq!(parsed.invalid_lines[0].line, 2);
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
        assert!(md.contains("## Mandatory Perf Checklist"));
        assert!(md.contains("## Summary"));
        assert!(md.contains("sqlite3:inserts:db-a:c4"));
        assert!(md.contains("Latency (ms)"));
        assert!(md.contains("Throughput (ops/sec)"));
        assert!(md.contains("Iteration details"));
    }

    #[test]
    fn render_benchmark_summaries_includes_causal_scorecards() {
        let sqlite = scorecard_summary("sqlite_reference", 100);
        let single_writer = scorecard_summary("fsqlite_single_writer", 70);
        let mvcc = scorecard_summary("fsqlite_mvcc", 50);

        let md = render_benchmark_summaries_markdown(&[sqlite, single_writer, mvcc]);
        assert!(md.contains("## Causal Scorecards"));
        assert!(md.contains("shared_fixed_tax_reduction"));
        assert!(md.contains("mvcc_concurrency_routing"));
        assert!(md.contains("Share of Total Gain"));
        assert!(md.contains("Counter deltas"));
        assert!(md.contains("Confirm each optimization claim names the comparator row"));
    }

    #[test]
    fn render_benchmark_summaries_surfaces_critical_benchmark_gates() {
        let mut c1_sqlite = make_benchmark_summary(
            "sqlite_reference",
            "fixture",
            "commutative_inserts_disjoint_keys",
            1,
        );
        c1_sqlite.throughput.median_ops_per_sec = 100.0;
        c1_sqlite.latency.median_ms = 1.0;
        c1_sqlite.latency.p95_ms = 1.0;
        c1_sqlite.latency.p99_ms = 1.0;

        let mut c1_mvcc = make_benchmark_summary(
            "fsqlite_mvcc",
            "fixture",
            "commutative_inserts_disjoint_keys",
            1,
        );
        c1_mvcc.throughput.median_ops_per_sec = 80.0;
        c1_mvcc.latency.median_ms = 1.2;
        c1_mvcc.latency.p95_ms = 1.3;
        c1_mvcc.latency.p99_ms = 1.4;

        let mut persistent_sqlite = make_benchmark_summary(
            "sqlite_reference",
            "fixture",
            "persistent_concurrent_write_16t",
            16,
        );
        persistent_sqlite.throughput.median_ops_per_sec = 100.0;
        persistent_sqlite.latency.median_ms = 1.0;
        persistent_sqlite.latency.p95_ms = 1.0;
        persistent_sqlite.latency.p99_ms = 1.0;

        let mut persistent_mvcc = make_benchmark_summary(
            "fsqlite_mvcc",
            "fixture",
            "persistent_concurrent_write_16t",
            16,
        );
        persistent_mvcc.throughput.median_ops_per_sec = 120.0;
        persistent_mvcc.latency.median_ms = 2.0;
        persistent_mvcc.latency.p95_ms = 3.0;
        persistent_mvcc.latency.p99_ms = 4.0;

        let md = render_benchmark_summaries_markdown(&[
            c1_sqlite,
            c1_mvcc,
            persistent_sqlite,
            persistent_mvcc,
        ]);

        assert!(md.contains("## Critical Benchmark Gates"));
        assert!(md.contains("c1_fixed_tax"));
        assert!(md.contains("persistent_concurrent_write_16t"));
        assert!(md.contains("below_parity"));
        assert!(md.contains("tail_slower_than_sqlite"));
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
        let mut summary = run_benchmark(&bench_config, &meta, |_| {
            Ok::<_, String>(dummy_engine_report(wall_ms, 1000))
        });
        let mode_id = match engine {
            "sqlite3" | "sqlite_reference" => "sqlite_reference",
            "fsqlite_single_writer" => "fsqlite_single_writer",
            _ => "fsqlite_mvcc",
        };
        let comparison = BenchmarkComparisonMetadata::anonymous(&summary, mode_id);
        summary.comparison = Some(comparison);
        summary
    }

    fn make_cell_outcome(engine: &str, fixture: &str, workload: &str, c: u16) -> CellOutcome {
        let mode_id = match engine {
            "sqlite3" | "sqlite_reference" => "sqlite_reference",
            "fsqlite_single_writer" => "fsqlite_single_writer",
            _ => "fsqlite_mvcc",
        };
        CellOutcome {
            summary: Some(make_benchmark_summary(engine, fixture, workload, c)),
            error: None,
            engine: engine.to_owned(),
            mode_id: Some(mode_id.to_owned()),
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
        assert!(md.contains("| Busy timeout (ms) | 0 |"));
        assert!(md.contains("| Seed | 42 |"));
        assert!(md.contains("| Scale | 100 |"));
        assert!(md.contains("`PRAGMA busy_timeout=0`"));
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
        assert!(
            md.contains("## Mode Comparison Summary"),
            "missing mode comparison section"
        );
        assert!(md.contains("sqlite med (ms)"), "missing sqlite column");
        assert!(md.contains("mvcc med (ms)"), "missing mvcc column");
    }

    #[test]
    fn perf_report_speedup_summary_omits_zero_denominator() {
        let sqlite_cell = make_cell_outcome("sqlite3", "db-a", "inserts", 1);
        let mut fsqlite_cell = make_cell_outcome("fsqlite", "db-a", "inserts", 1);
        let summary = fsqlite_cell.summary.as_mut().unwrap();
        summary.latency.min_ms = 0.0;
        summary.latency.max_ms = 0.0;
        summary.latency.mean_ms = 0.0;
        summary.latency.median_ms = 0.0;
        summary.latency.p95_ms = 0.0;
        summary.latency.p99_ms = 0.0;

        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 2,
            success_count: 2,
            error_count: 0,
            cells: vec![sqlite_cell, fsqlite_cell],
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());

        assert!(!md.contains("inf"));
        assert!(!md.contains("0.00x"));
        assert!(md.contains("| db-a | inserts | 1 |"));
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
            mode_id: Some("sqlite_reference".to_owned()),
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

    #[test]
    fn perf_report_excludes_partial_failure_cells_from_comparison_tables() {
        let mut sqlite_cell = make_cell_outcome("sqlite3", "db-a", "inserts", 1);
        sqlite_cell.error =
            Some("1/3 measurement iterations failed; first=setup failed".to_owned());
        let fsqlite_cell = make_cell_outcome("fsqlite", "db-a", "inserts", 1);

        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 2,
            success_count: 1,
            error_count: 1,
            cells: vec![sqlite_cell, fsqlite_cell],
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());

        assert!(md.contains("## Errors"));
        assert!(md.contains("1/3 measurement iterations failed; first=setup failed"));
        assert!(!md.contains("| sqlite3 | inserts | 1 |"));
        assert!(md.contains("| fsqlite_mvcc | inserts | 1 |"));
    }

    #[test]
    fn perf_report_scaling_analysis_excludes_partial_failure_cells() {
        let baseline = make_cell_outcome("fsqlite", "db-a", "inserts", 1);
        let mut partial_failure = make_cell_outcome("fsqlite", "db-a", "inserts", 4);
        partial_failure.error =
            Some("1/3 measurement iterations failed; first=worker stalled".to_owned());

        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 2,
            success_count: 1,
            error_count: 1,
            cells: vec![baseline, partial_failure],
        };
        let md = render_perf_report_markdown(&result, &default_perf_report_config());

        assert!(!md.contains("## Concurrency Scaling"));
        assert!(md.contains("1/3 measurement iterations failed; first=worker stalled"));
    }
}
