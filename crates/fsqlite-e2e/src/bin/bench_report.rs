//! Benchmark results collector and Markdown comparison report.
//!
//! Bead: bd-1wrv
//!
//! Reads Criterion JSON output from `target/criterion/` directories,
//! pairs FrankenSQLite and C SQLite results by benchmark group, computes
//! speedup ratios, and outputs both a Markdown summary table and JSON
//! for TUI dashboard consumption.
//!
//! Usage:
//!   cargo run -p fsqlite-e2e --bin bench-report [-- --criterion-dir <path>] [--json]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

// ─── Data structures ────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct Estimate {
    point_estimate: f64,
}

#[derive(serde::Deserialize)]
struct Estimates {
    mean: Estimate,
    median: Estimate,
    std_dev: Estimate,
}

#[derive(serde::Deserialize)]
struct BenchmarkId {
    group_id: String,
    function_id: Option<String>,
    #[allow(dead_code)]
    throughput: Option<serde_json::Value>,
}

#[derive(serde::Serialize)]
struct BenchResult {
    group: String,
    function: String,
    mean_ns: f64,
    median_ns: f64,
    std_dev_ns: f64,
}

#[derive(serde::Serialize)]
struct Comparison {
    group: String,
    csqlite_mean_ns: f64,
    frank_mean_ns: f64,
    speedup: f64,
    csqlite_median_ns: f64,
    frank_median_ns: f64,
}

#[derive(serde::Serialize)]
struct Report {
    generated_at: String,
    comparisons: Vec<Comparison>,
    unpaired: Vec<BenchResult>,
}

// ─── Parsing ────────────────────────────────────────────────────────────

fn parse_bench_result(dir: &Path) -> Option<BenchResult> {
    let new_dir = dir.join("new");
    if !new_dir.is_dir() {
        return None;
    }

    let estimates_path = new_dir.join("estimates.json");
    let benchmark_path = new_dir.join("benchmark.json");

    if !estimates_path.is_file() || !benchmark_path.is_file() {
        return None;
    }

    let estimates_str = std::fs::read_to_string(&estimates_path).ok()?;
    let benchmark_str = std::fs::read_to_string(&benchmark_path).ok()?;

    let estimates: Estimates = serde_json::from_str(&estimates_str).ok()?;
    let benchmark: BenchmarkId = serde_json::from_str(&benchmark_str).ok()?;

    Some(BenchResult {
        group: benchmark.group_id,
        function: benchmark.function_id.unwrap_or_default(),
        mean_ns: estimates.mean.point_estimate,
        median_ns: estimates.median.point_estimate,
        std_dev_ns: estimates.std_dev.point_estimate,
    })
}

fn discover_results(criterion_dir: &Path) -> Vec<BenchResult> {
    let mut results = Vec::new();

    let Ok(entries) = std::fs::read_dir(criterion_dir) else {
        return results;
    };

    for entry in entries.flatten() {
        let group_dir = entry.path();
        if !group_dir.is_dir() {
            continue;
        }

        // Each group directory may contain function subdirectories.
        let Ok(sub_entries) = std::fs::read_dir(&group_dir) else {
            continue;
        };

        for sub in sub_entries.flatten() {
            let sub_path = sub.path();
            if !sub_path.is_dir() {
                continue;
            }
            let name = sub_path.file_name().unwrap_or_default().to_string_lossy();
            // Skip report directories.
            if name == "report" {
                continue;
            }

            if let Some(result) = parse_bench_result(&sub_path) {
                results.push(result);
            }
        }
    }

    results
}

// ─── Comparison logic ───────────────────────────────────────────────────

fn build_comparisons(results: Vec<BenchResult>) -> (Vec<Comparison>, Vec<BenchResult>) {
    // Group by benchmark group name.
    let mut by_group: BTreeMap<String, Vec<BenchResult>> = BTreeMap::new();
    for r in results {
        by_group.entry(r.group.clone()).or_default().push(r);
    }

    let mut comparisons = Vec::new();
    let mut unpaired = Vec::new();

    for entries in by_group.values() {
        let csqlite: Vec<&BenchResult> = entries
            .iter()
            .filter(|e| is_csqlite_label(&e.function))
            .collect();
        let frank: Vec<&BenchResult> = entries
            .iter()
            .filter(|e| is_frank_label(&e.function))
            .collect();

        if csqlite.len() == 1 && frank.len() == 1 {
            let c = csqlite[0];
            let f = frank[0];
            let speedup = if f.mean_ns > 0.0 {
                c.mean_ns / f.mean_ns
            } else {
                0.0
            };
            comparisons.push(Comparison {
                group: c.group.clone(),
                csqlite_mean_ns: c.mean_ns,
                frank_mean_ns: f.mean_ns,
                speedup,
                csqlite_median_ns: c.median_ns,
                frank_median_ns: f.median_ns,
            });
        } else {
            // Unpaired — no matching pair found.
            for e in entries {
                unpaired.push(BenchResult {
                    group: e.group.clone(),
                    function: e.function.clone(),
                    mean_ns: e.mean_ns,
                    median_ns: e.median_ns,
                    std_dev_ns: e.std_dev_ns,
                });
            }
        }
    }

    (comparisons, unpaired)
}

fn is_csqlite_label(s: &str) -> bool {
    let lower = s.to_lowercase();
    lower.contains("csqlite") || lower.contains("c_sqlite") || lower == "sqlite"
}

fn is_frank_label(s: &str) -> bool {
    let lower = s.to_lowercase();
    lower.contains("frank") || lower.contains("fsqlite")
}

// ─── Formatting ─────────────────────────────────────────────────────────

fn format_ns(ns: f64) -> String {
    if ns >= 1_000_000_000.0 {
        format!("{:.2}s", ns / 1_000_000_000.0)
    } else if ns >= 1_000_000.0 {
        format!("{:.2}ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.1}\u{00b5}s", ns / 1_000.0)
    } else {
        format!("{ns:.0}ns")
    }
}

fn render_markdown(report: &Report) -> String {
    let mut md = String::new();

    md.push_str("# FrankenSQLite vs C SQLite Benchmark Report\n\n");
    let _ = write!(md, "Generated: {}\n\n", report.generated_at);

    if report.comparisons.is_empty() {
        md.push_str("*No paired benchmark results found.*\n\n");
        md.push_str("Run benchmarks first:\n```\n");
        md.push_str("cargo bench -p fsqlite-e2e\n```\n");
        return md;
    }

    md.push_str("## Summary Table\n\n");
    md.push_str("| Benchmark | C SQLite | FrankenSQLite | Speedup |\n");
    md.push_str("|-----------|----------|---------------|---------|\n");

    for c in &report.comparisons {
        let speedup_str = if c.speedup >= 1.0 {
            format!("{:.2}x", c.speedup)
        } else {
            format!("{:.2}x (slower)", c.speedup)
        };
        let _ = writeln!(
            md,
            "| {} | {} | {} | {} |",
            c.group,
            format_ns(c.csqlite_mean_ns),
            format_ns(c.frank_mean_ns),
            speedup_str,
        );
    }

    if !report.unpaired.is_empty() {
        md.push_str("\n## Unpaired Results\n\n");
        md.push_str("| Benchmark | Function | Mean |\n");
        md.push_str("|-----------|----------|------|\n");
        for u in &report.unpaired {
            let _ = writeln!(
                md,
                "| {} | {} | {} |",
                u.group,
                u.function,
                format_ns(u.mean_ns),
            );
        }
    }

    let _ = write!(
        md,
        "\n---\n*{} comparisons, {} unpaired results*\n",
        report.comparisons.len(),
        report.unpaired.len(),
    );

    md
}

// ─── Main ───────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut criterion_dir = PathBuf::from("target/criterion");
    let mut json_output = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--criterion-dir" => {
                i += 1;
                if i < args.len() {
                    criterion_dir = PathBuf::from(&args[i]);
                }
            }
            "--json" => {
                json_output = true;
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: bench-report [--criterion-dir <path>] [--json]\n\n\
                     Reads Criterion JSON output and generates a comparison report.\n\
                     Default criterion dir: target/criterion/"
                );
                return;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
            }
        }
        i += 1;
    }

    // Also check CARGO_TARGET_DIR env for non-standard target dirs.
    if !criterion_dir.is_dir()
        && let Ok(target) = std::env::var("CARGO_TARGET_DIR")
    {
        let alt = PathBuf::from(target).join("criterion");
        if alt.is_dir() {
            criterion_dir = alt;
        }
    }

    if !criterion_dir.is_dir() {
        eprintln!(
            "Criterion output directory not found: {}\n\
             Run benchmarks first: cargo bench -p fsqlite-e2e",
            criterion_dir.display()
        );
        std::process::exit(1);
    }

    let results = discover_results(&criterion_dir);
    if results.is_empty() {
        eprintln!("No benchmark results found in {}", criterion_dir.display());
        std::process::exit(1);
    }

    let (comparisons, unpaired) = build_comparisons(results);

    let report = Report {
        generated_at: chrono_lite_now(),
        comparisons,
        unpaired,
    };

    if json_output {
        let json = serde_json::to_string_pretty(&report).expect("JSON serialization failed");
        println!("{json}");
    } else {
        let md = render_markdown(&report);
        print!("{md}");
    }
}

/// Simple UTC timestamp without pulling in chrono.
fn chrono_lite_now() -> String {
    // Use /proc/uptime-independent approach: just format current time.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    // Convert to approximate date (good enough for a report header).
    let days = secs / 86400;
    let year_approx = 1970 + days / 365;
    format!("{year_approx}-xx-xx (epoch: {secs})")
}
