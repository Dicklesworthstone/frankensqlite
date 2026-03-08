//! Comprehensive FrankenSQLite vs C SQLite benchmark.
//!
//! Measures insertion throughput across multiple dimensions:
//!
//! **Row counts:** 100, 1K, 10K, 100K
//! **Record sizes:** tiny (1 col), small (3 cols), medium (6 cols), large (10 cols with ~500B text)
//! **Transaction strategies:** autocommit, batched (1K per txn), single txn
//! **Concurrency:** single writer, 2/4/8 concurrent writers (C SQLite WAL vs FrankenSQLite MVCC)
//! **Read-after-write:** full scan, point lookup, range scan, COUNT(*), indexed lookup
//!
//! Usage:
//!   cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench
//!   cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --quick
//!   cargo run --profile release-perf -p fsqlite-e2e --bin comprehensive-bench -- --filter insert

use std::io::Write as _;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

// ─── Configuration ─────────────────────────────────────────────────────

const WARMUP_ITERS: usize = 2;
const MIN_ITERS: usize = 3;
const MAX_ITERS: usize = 10;
const TARGET_DURATION: Duration = Duration::from_secs(5);

const ROW_COUNTS: &[usize] = &[100, 1_000, 10_000, 100_000];
const ROW_COUNTS_QUICK: &[usize] = &[100, 1_000, 10_000];

const CONCURRENT_THREAD_COUNTS: &[usize] = &[2, 4, 8];
const CONCURRENT_ROWS_PER_THREAD: usize = 1_000;
const CONCURRENT_RANGE_SIZE: i64 = 1_000_000;

// ─── Record size definitions ───────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum RecordSize {
    Tiny,
    Small,
    Medium,
    Large,
}

impl RecordSize {
    const ALL: &[Self] = &[Self::Tiny, Self::Small, Self::Medium, Self::Large];

    const fn name(self) -> &'static str {
        match self {
            Self::Tiny => "tiny_1col",
            Self::Small => "small_3col",
            Self::Medium => "medium_6col",
            Self::Large => "large_10col",
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::Tiny => "1 col (INTEGER PK only)",
            Self::Small => "3 cols (~30B: id, name, value)",
            Self::Medium => "6 cols (~180B: id, name, email, bio, category, score)",
            Self::Large => "10 cols (~600B: includes long text fields)",
        }
    }

    fn create_table_sql(self) -> &'static str {
        match self {
            Self::Tiny => "CREATE TABLE bench (id INTEGER PRIMARY KEY)",
            Self::Small => {
                "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL NOT NULL)"
            }
            Self::Medium => {
                "CREATE TABLE bench (\
                id INTEGER PRIMARY KEY, \
                first_name TEXT NOT NULL, \
                last_name TEXT NOT NULL, \
                email TEXT NOT NULL, \
                bio TEXT NOT NULL, \
                score INTEGER NOT NULL\
            )"
            }
            Self::Large => {
                "CREATE TABLE bench (\
                id INTEGER PRIMARY KEY, \
                first_name TEXT NOT NULL, \
                last_name TEXT NOT NULL, \
                email TEXT NOT NULL, \
                department TEXT NOT NULL, \
                title TEXT NOT NULL, \
                bio TEXT NOT NULL, \
                address TEXT NOT NULL, \
                notes TEXT NOT NULL, \
                score INTEGER NOT NULL\
            )"
            }
        }
    }

    fn insert_sql_csqlite(self) -> &'static str {
        match self {
            Self::Tiny => "INSERT INTO bench VALUES (?1)",
            Self::Small => "INSERT INTO bench VALUES (?1, ('user_' || ?1), (?1 * 0.137))",
            Self::Medium => {
                "INSERT INTO bench VALUES (\
                ?1, \
                ('Alice_' || ?1), \
                ('Smith_' || ?1), \
                ('user' || ?1 || '@example.com'), \
                ('Bio text for user number ' || ?1 || '. This is a medium-length description that adds some realistic payload to each row in the database.'), \
                (?1 * 7)\
            )"
            }
            Self::Large => {
                "INSERT INTO bench VALUES (\
                ?1, \
                ('FirstName_' || ?1), \
                ('LastName_' || ?1), \
                ('employee' || ?1 || '@bigcorp.example.com'), \
                ('Engineering_Dept_' || (?1 % 20)), \
                ('Senior Software Engineer Level ' || (?1 % 5)), \
                ('This is the biography for employee number ' || ?1 || '. They have been working at the company for many years and have contributed to numerous projects across multiple teams. Their expertise spans distributed systems, database internals, and performance optimization. They are known for their thorough code reviews and mentorship of junior engineers.'), \
                (?1 || ' Technology Park, Building ' || (?1 % 50) || ', Suite ' || (?1 % 200) || ', Innovation City, CA 94000'), \
                ('Internal notes: Employee ' || ?1 || ' - Performance rating: Exceeds Expectations. Last review date: 2026-01-15. Next review: 2026-07-15. Skills: Rust, C++, SQL, distributed systems, leadership.'), \
                (?1 * 13)\
                )"
            }
        }
    }

    fn insert_sql_fsqlite(self, i: i64) -> String {
        match self {
            Self::Tiny => format!("INSERT INTO bench VALUES ({i})"),
            Self::Small => {
                format!(
                    "INSERT INTO bench VALUES ({i}, 'user_{i}', {})",
                    f64::from(i as i32) * 0.137
                )
            }
            Self::Medium => format!(
                "INSERT INTO bench VALUES (\
                    {i}, \
                    'Alice_{i}', \
                    'Smith_{i}', \
                    'user{i}@example.com', \
                    'Bio text for user number {i}. This is a medium-length description that adds some realistic payload to each row in the database.', \
                    {}\
                )",
                i * 7,
            ),
            Self::Large => format!(
                "INSERT INTO bench VALUES (\
                    {i}, \
                    'FirstName_{i}', \
                    'LastName_{i}', \
                    'employee{i}@bigcorp.example.com', \
                    'Engineering_Dept_{}', \
                    'Senior Software Engineer Level {}', \
                    'This is the biography for employee number {i}. They have been working at the company for many years and have contributed to numerous projects across multiple teams. Their expertise spans distributed systems, database internals, and performance optimization. They are known for their thorough code reviews and mentorship of junior engineers.', \
                    '{i} Technology Park, Building {}, Suite {}, Innovation City, CA 94000', \
                    'Internal notes: Employee {i} - Performance rating: Exceeds Expectations. Last review date: 2026-01-15. Next review: 2026-07-15. Skills: Rust, C++, SQL, distributed systems, leadership.', \
                    {}\
                )",
                i % 20,
                i % 5,
                i % 50,
                i % 200,
                i * 13,
            ),
        }
    }
}

// ─── Measurement infrastructure ────────────────────────────────────────

#[allow(dead_code)]
#[derive(Clone)]
struct Measurement {
    label: String,
    durations: Vec<Duration>,
    row_count: usize,
}

#[allow(dead_code)]
impl Measurement {
    fn mean(&self) -> Duration {
        let total: Duration = self.durations.iter().sum();
        total / u32::try_from(self.durations.len()).unwrap_or(1)
    }

    fn median(&self) -> Duration {
        let mut sorted: Vec<Duration> = self.durations.clone();
        sorted.sort();
        sorted[sorted.len() / 2]
    }

    fn min(&self) -> Duration {
        self.durations.iter().copied().min().unwrap_or_default()
    }

    fn stddev(&self) -> Duration {
        let mean = self.mean().as_nanos() as f64;
        let variance = self
            .durations
            .iter()
            .map(|d| {
                let diff = d.as_nanos() as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / self.durations.len() as f64;
        Duration::from_nanos(variance.sqrt() as u64)
    }

    fn rows_per_sec(&self) -> f64 {
        let secs = self.median().as_secs_f64();
        if secs == 0.0 {
            return 0.0;
        }
        self.row_count as f64 / secs
    }

    fn us_per_row(&self) -> f64 {
        let us = self.median().as_secs_f64() * 1_000_000.0;
        if self.row_count == 0 {
            return 0.0;
        }
        us / self.row_count as f64
    }
}

fn measure<F: FnMut()>(label: &str, row_count: usize, mut f: F) -> Measurement {
    // Warmup
    for _ in 0..WARMUP_ITERS {
        f();
    }

    let mut durations = Vec::new();
    let mut total_elapsed = Duration::ZERO;

    for iter in 0..MAX_ITERS {
        let start = Instant::now();
        f();
        let elapsed = start.elapsed();
        durations.push(elapsed);
        total_elapsed += elapsed;

        if iter >= MIN_ITERS && total_elapsed >= TARGET_DURATION {
            break;
        }
    }

    Measurement {
        label: label.to_string(),
        durations,
        row_count,
    }
}

// ─── PRAGMA helpers ────────────────────────────────────────────────────

fn apply_pragmas_csqlite(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .ok();
}

fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        let _ = conn.execute(pragma);
    }
}

// ─── Report formatting ────────────────────────────────────────────────

struct BenchReport {
    sections: Vec<ReportSection>,
}

struct ReportSection {
    title: String,
    description: String,
    rows: Vec<ReportRow>,
}

struct ReportRow {
    scenario: String,
    csqlite: Option<Measurement>,
    fsqlite: Option<Measurement>,
}

impl BenchReport {
    fn new() -> Self {
        Self {
            sections: Vec::new(),
        }
    }

    fn add_section(&mut self, title: &str, description: &str) -> &mut ReportSection {
        self.sections.push(ReportSection {
            title: title.to_string(),
            description: description.to_string(),
            rows: Vec::new(),
        });
        self.sections.last_mut().unwrap()
    }

    fn print(&self) {
        println!("\n{}", "=".repeat(120));
        println!("  COMPREHENSIVE BENCHMARK: FrankenSQLite vs C SQLite");
        println!("  {}", chrono_stamp());
        println!("{}\n", "=".repeat(120));

        for section in &self.sections {
            println!("\n## {}", section.title);
            if !section.description.is_empty() {
                println!("   {}\n", section.description);
            }

            // Header
            println!(
                "  {:<45} {:>14} {:>14} {:>14} {:>14} {:>10}",
                "Scenario", "C SQLite", "FrankenSQLite", "C rows/s", "F rows/s", "Ratio"
            );
            println!("  {}", "-".repeat(115));

            for row in &section.rows {
                let cs_time = row
                    .csqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format_duration(m.median()));
                let fs_time = row
                    .fsqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format_duration(m.median()));
                let cs_rps = row
                    .csqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format_rps(m.rows_per_sec()));
                let fs_rps = row
                    .fsqlite
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |m| format_rps(m.rows_per_sec()));

                let ratio = match (&row.csqlite, &row.fsqlite) {
                    (Some(c), Some(f)) => {
                        let r = f.median().as_nanos() as f64 / c.median().as_nanos() as f64;
                        if r < 1.0 {
                            format!("{:.2}x \x1b[32mfaster\x1b[0m", 1.0 / r)
                        } else if r > 1.0 {
                            format!("{:.2}x \x1b[31mslower\x1b[0m", r)
                        } else {
                            "1.00x equal".to_string()
                        }
                    }
                    _ => "N/A".to_string(),
                };

                println!(
                    "  {:<45} {:>14} {:>14} {:>14} {:>14} {:>10}",
                    row.scenario, cs_time, fs_time, cs_rps, fs_rps, ratio
                );
            }
        }

        // Summary statistics
        println!("\n{}", "=".repeat(120));
        println!("  SUMMARY STATISTICS");
        println!("{}\n", "=".repeat(120));

        let mut faster_count = 0_usize;
        let mut slower_count = 0_usize;
        let mut equal_count = 0_usize;
        let mut total_ratio = 0.0_f64;
        let mut ratio_count = 0_usize;

        for section in &self.sections {
            for row in &section.rows {
                if let (Some(c), Some(f)) = (&row.csqlite, &row.fsqlite) {
                    let r = f.median().as_nanos() as f64 / c.median().as_nanos() as f64;
                    total_ratio += r;
                    ratio_count += 1;
                    if r < 0.95 {
                        faster_count += 1;
                    } else if r > 1.05 {
                        slower_count += 1;
                    } else {
                        equal_count += 1;
                    }
                }
            }
        }

        if ratio_count > 0 {
            let avg_ratio = total_ratio / ratio_count as f64;
            println!(
                "  Total scenarios: {}  |  FrankenSQLite faster: {}  |  Comparable: {}  |  C SQLite faster: {}",
                ratio_count, faster_count, equal_count, slower_count
            );
            println!(
                "  Average time ratio (FrankenSQLite / C SQLite): {:.2}x",
                avg_ratio
            );
        }

        println!();
    }

    fn write_html(&self, path: &str) {
        let mut html = String::with_capacity(32 * 1024);

        // Collect JSON data for charts.
        let mut sections_json = String::from("[");
        for (si, section) in self.sections.iter().enumerate() {
            if si > 0 {
                sections_json.push(',');
            }
            sections_json.push_str(&format!(
                r#"{{"title":{},"description":{},"rows":["#,
                json_string(&section.title),
                json_string(&section.description),
            ));
            for (ri, row) in section.rows.iter().enumerate() {
                if ri > 0 {
                    sections_json.push(',');
                }
                let cs_median_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.median().as_nanos() as f64);
                let fs_median_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.median().as_nanos() as f64);
                let cs_rps = row.csqlite.as_ref().map_or(0.0, Measurement::rows_per_sec);
                let fs_rps = row.fsqlite.as_ref().map_or(0.0, Measurement::rows_per_sec);
                let cs_mean_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.mean().as_nanos() as f64);
                let fs_mean_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.mean().as_nanos() as f64);
                let cs_min_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.min().as_nanos() as f64);
                let fs_min_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.min().as_nanos() as f64);
                let cs_stddev_ns = row
                    .csqlite
                    .as_ref()
                    .map_or(0.0, |m| m.stddev().as_nanos() as f64);
                let fs_stddev_ns = row
                    .fsqlite
                    .as_ref()
                    .map_or(0.0, |m| m.stddev().as_nanos() as f64);
                let cs_iters = row.csqlite.as_ref().map_or(0, |m| m.durations.len());
                let fs_iters = row.fsqlite.as_ref().map_or(0, |m| m.durations.len());
                let ratio = if cs_median_ns > 0.0 {
                    fs_median_ns / cs_median_ns
                } else {
                    0.0
                };
                sections_json.push_str(&format!(
                    r#"{{"scenario":{},"cs_median_ns":{cs_median_ns},"fs_median_ns":{fs_median_ns},"cs_rps":{cs_rps},"fs_rps":{fs_rps},"cs_mean_ns":{cs_mean_ns},"fs_mean_ns":{fs_mean_ns},"cs_min_ns":{cs_min_ns},"fs_min_ns":{fs_min_ns},"cs_stddev_ns":{cs_stddev_ns},"fs_stddev_ns":{fs_stddev_ns},"cs_iters":{cs_iters},"fs_iters":{fs_iters},"ratio":{ratio}}}"#,
                    json_string(&row.scenario),
                ));
            }
            sections_json.push_str("]}");
        }
        sections_json.push(']');

        // Summary stats.
        let mut faster = 0_usize;
        let mut slower = 0_usize;
        let mut comparable = 0_usize;
        let mut total_ratio = 0.0_f64;
        let mut count = 0_usize;
        for section in &self.sections {
            for row in &section.rows {
                if let (Some(c), Some(f)) = (&row.csqlite, &row.fsqlite) {
                    let r = f.median().as_nanos() as f64 / c.median().as_nanos() as f64;
                    total_ratio += r;
                    count += 1;
                    if r < 0.95 {
                        faster += 1;
                    } else if r > 1.05 {
                        slower += 1;
                    } else {
                        comparable += 1;
                    }
                }
            }
        }
        let avg_ratio = if count > 0 {
            total_ratio / count as f64
        } else {
            1.0
        };

        html.push_str(&format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FrankenSQLite vs C SQLite — Benchmark Report</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<script>
tailwind.config = {{
  theme: {{
    extend: {{
      fontFamily: {{
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      }},
    }},
  }},
}}
</script>
<style>
  body {{ font-family: 'Inter', system-ui, sans-serif; }}
  .gradient-bg {{ background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%); }}
  .card {{ background: rgba(30, 41, 59, 0.8); backdrop-filter: blur(12px); border: 1px solid rgba(148, 163, 184, 0.1); }}
  .glow {{ box-shadow: 0 0 40px rgba(59, 130, 246, 0.15); }}
  .stat-card {{ transition: transform 0.2s, box-shadow 0.2s; }}
  .stat-card:hover {{ transform: translateY(-2px); box-shadow: 0 8px 30px rgba(0,0,0,0.3); }}
  .faster {{ color: #34d399; }}
  .slower {{ color: #f87171; }}
  .equal {{ color: #94a3b8; }}
  .bar-cs {{ background: linear-gradient(90deg, #3b82f6, #60a5fa); }}
  .bar-fs {{ background: linear-gradient(90deg, #f59e0b, #fbbf24); }}
  table th {{ position: sticky; top: 0; z-index: 10; }}
  .section-nav a {{ transition: all 0.15s; }}
  .section-nav a:hover {{ background: rgba(59, 130, 246, 0.2); }}
  .section-nav a.active {{ background: rgba(59, 130, 246, 0.3); border-left-color: #3b82f6; }}
</style>
</head>
<body class="gradient-bg min-h-screen text-slate-200">

<!-- Hero Header -->
<header class="py-12 px-6 text-center border-b border-slate-700/50">
  <div class="max-w-5xl mx-auto">
    <div class="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-blue-500/10 border border-blue-500/20 text-blue-400 text-sm font-medium mb-6">
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
      Performance Benchmark Report
    </div>
    <h1 class="text-4xl md:text-5xl font-extrabold bg-gradient-to-r from-white via-slate-200 to-slate-400 bg-clip-text text-transparent mb-4">
      FrankenSQLite vs C SQLite
    </h1>
    <p class="text-slate-400 text-lg max-w-2xl mx-auto">
      Comprehensive comparison across insertions, reads, concurrency, and mixed workloads.
      MVCC page-level versioning vs traditional WAL write lock.
    </p>
    <p class="text-slate-500 text-sm mt-4 font-mono">{}</p>
  </div>
</header>

<!-- Summary Cards -->
<section class="max-w-6xl mx-auto px-6 -mt-6">
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
    <div class="card rounded-xl p-5 stat-card glow">
      <div class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">Total Scenarios</div>
      <div class="text-3xl font-bold text-white">{count}</div>
    </div>
    <div class="card rounded-xl p-5 stat-card" style="box-shadow: 0 0 40px rgba(52,211,153,0.12);">
      <div class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">FrankenSQLite Faster</div>
      <div class="text-3xl font-bold faster">{faster}</div>
    </div>
    <div class="card rounded-xl p-5 stat-card">
      <div class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">Comparable</div>
      <div class="text-3xl font-bold equal">{comparable}</div>
    </div>
    <div class="card rounded-xl p-5 stat-card" style="box-shadow: 0 0 40px rgba(248,113,113,0.10);">
      <div class="text-xs font-medium text-slate-400 uppercase tracking-wider mb-1">C SQLite Faster</div>
      <div class="text-3xl font-bold slower">{slower}</div>
    </div>
  </div>
  <div class="card rounded-xl p-5 mt-4 text-center">
    <span class="text-slate-400">Average time ratio (FrankenSQLite / C SQLite):</span>
    <span class="text-xl font-bold ml-2 {}">{avg_ratio:.2}x</span>
  </div>
</section>

<!-- Section Navigation -->
<nav class="section-nav max-w-6xl mx-auto px-6 mt-8">
  <div class="card rounded-xl p-4 flex flex-wrap gap-2" id="section-nav"></div>
</nav>

<!-- Benchmark Sections -->
<main class="max-w-6xl mx-auto px-6 py-8 space-y-10" id="sections-container"></main>

<!-- Footer -->
<footer class="border-t border-slate-700/50 py-8 text-center text-slate-500 text-sm">
  <p>Generated by <span class="text-slate-300 font-medium">comprehensive-bench</span> &mdash; FrankenSQLite E2E Benchmark Suite</p>
  <p class="mt-1">Clean-room Rust reimplementation of SQLite with MVCC page-level versioning</p>
</footer>

<script>
const DATA = {sections_json};

function fmtDuration(ns) {{
  if (ns === 0) return 'N/A';
  if (ns < 1e3) return ns.toFixed(0) + ' ns';
  if (ns < 1e6) return (ns / 1e3).toFixed(1) + ' \u00b5s';
  if (ns < 1e9) return (ns / 1e6).toFixed(2) + ' ms';
  return (ns / 1e9).toFixed(3) + ' s';
}}

function fmtRps(rps) {{
  if (rps === 0) return 'N/A';
  if (rps >= 1e6) return (rps / 1e6).toFixed(2) + 'M/s';
  if (rps >= 1e3) return (rps / 1e3).toFixed(1) + 'K/s';
  return rps.toFixed(0) + '/s';
}}

function ratioClass(r) {{
  if (r < 0.95) return 'faster';
  if (r > 1.05) return 'slower';
  return 'equal';
}}

function ratioText(r) {{
  if (r === 0) return 'N/A';
  if (r < 1.0) return (1/r).toFixed(2) + 'x faster';
  if (r > 1.0) return r.toFixed(2) + 'x slower';
  return '1.00x equal';
}}

// Build section navigation
const nav = document.getElementById('section-nav');
DATA.forEach((sec, i) => {{
  const a = document.createElement('a');
  a.href = '#section-' + i;
  a.textContent = sec.title.replace(/\u2014/g, '-').substring(0, 40) + (sec.title.length > 40 ? '...' : '');
  a.className = 'block px-3 py-1.5 rounded-lg text-sm text-slate-300 border-l-2 border-transparent hover:text-white cursor-pointer';
  nav.appendChild(a);
}});

// Build sections
const container = document.getElementById('sections-container');
DATA.forEach((sec, si) => {{
  const div = document.createElement('div');
  div.id = 'section-' + si;
  div.className = 'scroll-mt-24';

  // Only create chart for sections with paired data
  const hasChart = sec.rows.some(r => r.cs_median_ns > 0 && r.fs_median_ns > 0);
  const chartId = 'chart-' + si;

  let tableRows = '';
  sec.rows.forEach(r => {{
    const rc = ratioClass(r.ratio);
    tableRows += `<tr class="border-b border-slate-700/30 hover:bg-slate-700/20 transition-colors">
      <td class="py-3 px-4 text-sm font-medium text-slate-200">${{r.scenario}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-blue-400">${{fmtDuration(r.cs_median_ns)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-amber-400">${{fmtDuration(r.fs_median_ns)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-blue-300">${{fmtRps(r.cs_rps)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-amber-300">${{fmtRps(r.fs_rps)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right font-semibold ${{rc}}">${{ratioText(r.ratio)}}</td>
      <td class="py-3 px-4 text-sm font-mono text-right text-slate-500">${{fmtDuration(r.cs_stddev_ns)}} / ${{fmtDuration(r.fs_stddev_ns)}}</td>
    </tr>`;
  }});

  div.innerHTML = `
    <div class="card rounded-2xl overflow-hidden glow">
      <div class="px-6 py-5 border-b border-slate-700/50">
        <h2 class="text-xl font-bold text-white">${{sec.title}}</h2>
        ${{sec.description ? '<p class="text-sm text-slate-400 mt-1">' + sec.description + '</p>' : ''}}
      </div>
      ${{hasChart ? '<div class="px-6 py-4 border-b border-slate-700/30"><canvas id="' + chartId + '" height="' + Math.max(60, sec.rows.length * 28) + '"></canvas></div>' : ''}}
      <div class="overflow-x-auto">
        <table class="w-full text-left">
          <thead>
            <tr class="bg-slate-800/80 text-xs font-semibold text-slate-400 uppercase tracking-wider">
              <th class="py-3 px-4">Scenario</th>
              <th class="py-3 px-4 text-right">C SQLite</th>
              <th class="py-3 px-4 text-right">FrankenSQLite</th>
              <th class="py-3 px-4 text-right">C rows/s</th>
              <th class="py-3 px-4 text-right">F rows/s</th>
              <th class="py-3 px-4 text-right">Ratio</th>
              <th class="py-3 px-4 text-right">StdDev (C/F)</th>
            </tr>
          </thead>
          <tbody>${{tableRows}}</tbody>
        </table>
      </div>
    </div>`;

  container.appendChild(div);

  // Create horizontal bar chart
  if (hasChart) {{
    const ctx = document.getElementById(chartId).getContext('2d');
    const labels = sec.rows.filter(r => r.cs_median_ns > 0 && r.fs_median_ns > 0).map(r => {{
      const s = r.scenario;
      return s.length > 50 ? s.substring(0, 47) + '...' : s;
    }});
    const csData = sec.rows.filter(r => r.cs_median_ns > 0 && r.fs_median_ns > 0).map(r => r.cs_median_ns / 1e6);
    const fsData = sec.rows.filter(r => r.cs_median_ns > 0 && r.fs_median_ns > 0).map(r => r.fs_median_ns / 1e6);

    new Chart(ctx, {{
      type: 'bar',
      data: {{
        labels: labels,
        datasets: [
          {{
            label: 'C SQLite (ms)',
            data: csData,
            backgroundColor: 'rgba(59, 130, 246, 0.7)',
            borderColor: 'rgba(96, 165, 250, 1)',
            borderWidth: 1,
            borderRadius: 4,
          }},
          {{
            label: 'FrankenSQLite (ms)',
            data: fsData,
            backgroundColor: 'rgba(245, 158, 11, 0.7)',
            borderColor: 'rgba(251, 191, 36, 1)',
            borderWidth: 1,
            borderRadius: 4,
          }},
        ],
      }},
      options: {{
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{
          legend: {{
            labels: {{ color: '#94a3b8', font: {{ family: 'Inter', size: 12 }} }},
          }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                return ctx.dataset.label + ': ' + ctx.parsed.x.toFixed(2) + ' ms';
              }}
            }}
          }},
        }},
        scales: {{
          x: {{
            type: 'logarithmic',
            title: {{ display: true, text: 'Time (ms, log scale)', color: '#64748b' }},
            ticks: {{ color: '#64748b', font: {{ family: 'JetBrains Mono', size: 11 }} }},
            grid: {{ color: 'rgba(71, 85, 105, 0.3)' }},
          }},
          y: {{
            ticks: {{ color: '#94a3b8', font: {{ family: 'Inter', size: 11 }} }},
            grid: {{ display: false }},
          }},
        }},
      }},
    }});
  }}
}});

// Intersection observer for nav highlighting
const observer = new IntersectionObserver((entries) => {{
  entries.forEach(entry => {{
    if (entry.isIntersecting) {{
      const idx = entry.target.id.replace('section-', '');
      document.querySelectorAll('.section-nav a').forEach((a, i) => {{
        a.classList.toggle('active', i === parseInt(idx));
      }});
    }}
  }});
}}, {{ threshold: 0.3 }});
document.querySelectorAll('[id^="section-"]').forEach(el => observer.observe(el));
</script>
</body>
</html>"#,
            chrono_stamp(),
            if avg_ratio < 1.0 { "faster" } else { "slower" },
        ));

        let Ok(mut file) = std::fs::File::create(path) else {
            eprintln!("ERROR: Could not create HTML file at {path}");
            return;
        };
        if file.write_all(html.as_bytes()).is_ok() {
            eprintln!("HTML report written to: {path}");
        } else {
            eprintln!("ERROR: Failed to write HTML file");
        }
    }
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < '\x20' => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

impl ReportSection {
    fn add_row(
        &mut self,
        scenario: &str,
        csqlite: Option<Measurement>,
        fsqlite: Option<Measurement>,
    ) {
        self.rows.push(ReportRow {
            scenario: scenario.to_string(),
            csqlite,
            fsqlite,
        });
    }
}

fn format_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        format!("{:.1} us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.3} s", nanos as f64 / 1_000_000_000.0)
    }
}

fn format_rps(rps: f64) -> String {
    if rps >= 1_000_000.0 {
        format!("{:.2}M/s", rps / 1_000_000.0)
    } else if rps >= 1_000.0 {
        format!("{:.1}K/s", rps / 1_000.0)
    } else {
        format!("{:.0}/s", rps)
    }
}

fn chrono_stamp() -> String {
    // Simple timestamp without chrono dependency.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("Timestamp: {now} (unix)")
}

// ─── Section 1: Insert throughput by row count ─────────────────────────

fn bench_insert_by_row_count(
    report: &mut BenchReport,
    row_counts: &[usize],
    record_size: RecordSize,
) {
    let section = report.add_section(
        &format!(
            "INSERTThroughput — Single Transaction — {}",
            record_size.name()
        ),
        &format!(
            "Record: {}. All rows inserted in a single BEGIN..COMMIT.",
            record_size.description()
        ),
    );

    for &count in row_counts {
        eprint!(
            "  Benchmarking single-txn insert {count} rows ({})... ",
            record_size.name()
        );

        let csqlite_m = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("csqlite_{count}"), count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn.prepare(insert_sql).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            })
        };

        let fsqlite_m = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fsqlite_{count}"), count, || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(create_sql).unwrap();
                conn.execute("BEGIN").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    conn.execute(&record_size.insert_sql_fsqlite(i)).unwrap();
                }
                conn.execute("COMMIT").unwrap();
            })
        };

        eprintln!(
            "C={} F={}",
            format_duration(csqlite_m.median()),
            format_duration(fsqlite_m.median()),
        );

        section.add_row(&format!("{count} rows"), Some(csqlite_m), Some(fsqlite_m));
    }
}

// ─── Section 2: Insert throughput by transaction strategy ──────────────

fn bench_insert_by_txn_strategy(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "INSERTThroughput — Transaction Strategy Comparison (small_3col)",
        "Compares autocommit, batched (1K/txn), and single-txn strategies.",
    );

    let record_size = RecordSize::Small;

    for &count in row_counts {
        // Skip autocommit for large counts (too slow).
        let do_autocommit = count <= 10_000;
        let batch_size = 1000.min(count);

        // --- Autocommit ---
        if do_autocommit {
            eprint!("  Benchmarking autocommit {count} rows... ");

            let cs = {
                let insert_sql = record_size.insert_sql_csqlite();
                let create_sql = record_size.create_table_sql();
                measure(&format!("cs_auto_{count}"), count, || {
                    let conn = rusqlite::Connection::open_in_memory().unwrap();
                    apply_pragmas_csqlite(&conn);
                    conn.execute_batch(&format!("{create_sql};")).unwrap();
                    let mut stmt = conn.prepare(insert_sql).unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..count as i64 {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                })
            };

            let fs = {
                let create_sql = record_size.create_table_sql();
                measure(&format!("fs_auto_{count}"), count, || {
                    let conn = fsqlite::Connection::open(":memory:").unwrap();
                    apply_pragmas_fsqlite(&conn);
                    conn.execute(create_sql).unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..count as i64 {
                        conn.execute(&record_size.insert_sql_fsqlite(i)).unwrap();
                    }
                })
            };

            eprintln!(
                "C={} F={}",
                format_duration(cs.median()),
                format_duration(fs.median())
            );
            section.add_row(&format!("{count} rows / autocommit"), Some(cs), Some(fs));
        }

        // --- Batched ---
        eprint!("  Benchmarking batched {count} rows ({batch_size}/txn)... ");

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("cs_batch_{count}"), count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                let mut stmt = conn.prepare(insert_sql).unwrap();
                let num_batches = count.div_ceil(batch_size);
                #[allow(clippy::cast_possible_wrap)]
                for batch in 0..num_batches {
                    conn.execute_batch("BEGIN").unwrap();
                    let start = (batch * batch_size) as i64;
                    let end = ((batch + 1) * batch_size).min(count) as i64;
                    for i in start..end {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                    conn.execute_batch("COMMIT").unwrap();
                }
            })
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fs_batch_{count}"), count, || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(create_sql).unwrap();
                let num_batches = count.div_ceil(batch_size);
                #[allow(clippy::cast_possible_wrap)]
                for batch in 0..num_batches {
                    conn.execute("BEGIN").unwrap();
                    let start = (batch * batch_size) as i64;
                    let end = ((batch + 1) * batch_size).min(count) as i64;
                    for i in start..end {
                        conn.execute(&record_size.insert_sql_fsqlite(i)).unwrap();
                    }
                    conn.execute("COMMIT").unwrap();
                }
            })
        };

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / batched ({batch_size}/txn)"),
            Some(cs),
            Some(fs),
        );

        // --- Single txn ---
        eprint!("  Benchmarking single-txn {count} rows... ");

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("cs_txn_{count}"), count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn.prepare(insert_sql).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            })
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fs_txn_{count}"), count, || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(create_sql).unwrap();
                conn.execute("BEGIN").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    conn.execute(&record_size.insert_sql_fsqlite(i)).unwrap();
                }
                conn.execute("COMMIT").unwrap();
            })
        };

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(&format!("{count} rows / single txn"), Some(cs), Some(fs));
    }
}

// ─── Section 3: Insert throughput by record size ───────────────────────

fn bench_insert_by_record_size(report: &mut BenchReport) {
    let section = report.add_section(
        "INSERTThroughput — Record Size Comparison (10K rows, single txn)",
        "Fixed 10K rows in a single transaction, varying payload size.",
    );

    let count = 10_000_usize;

    for &record_size in RecordSize::ALL {
        eprint!(
            "  Benchmarking 10K rows record size {}... ",
            record_size.name()
        );

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("cs_{}", record_size.name()), count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn.prepare(insert_sql).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            })
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fs_{}", record_size.name()), count, || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(create_sql).unwrap();
                conn.execute("BEGIN").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    conn.execute(&record_size.insert_sql_fsqlite(i)).unwrap();
                }
                conn.execute("COMMIT").unwrap();
            })
        };

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{} — {}", record_size.name(), record_size.description()),
            Some(cs),
            Some(fs),
        );
    }
}

// ─── Section 4: Concurrent writers ─────────────────────────────────────

fn bench_concurrent_writers(report: &mut BenchReport) {
    let section = report.add_section(
        "Concurrent Writers — C SQLite WAL vs FrankenSQLite MVCC",
        &format!(
            "Each writer inserts {} rows into non-overlapping key ranges. \
             C SQLite uses file-backed WAL with busy_timeout; FrankenSQLite uses in-memory MVCC. \
             FrankenSQLite currently runs writers sequentially on one connection (MVCC path).",
            CONCURRENT_ROWS_PER_THREAD
        ),
    );

    for &n_threads in CONCURRENT_THREAD_COUNTS {
        let total_rows = n_threads * CONCURRENT_ROWS_PER_THREAD;
        eprint!("  Benchmarking {n_threads} concurrent writers ({total_rows} total rows)... ");

        // C SQLite: file-backed WAL with multiple connections.
        let cs = measure(&format!("cs_concurrent_{n_threads}t"), total_rows, || {
            let tmp = tempfile::NamedTempFile::new().unwrap();
            let path = tmp.path().to_str().unwrap().to_owned();
            {
                let setup = rusqlite::Connection::open(&path).unwrap();
                setup
                    .execute_batch(
                        "PRAGMA page_size = 4096;\
                         PRAGMA journal_mode = WAL;\
                         PRAGMA synchronous = NORMAL;\
                         PRAGMA cache_size = -64000;\
                         CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
                    )
                    .unwrap();
            }

            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = path.clone();
                    let bar = barrier.clone();
                    std::thread::spawn(move || {
                        let conn = rusqlite::Connection::open(&p).unwrap();
                        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                            .unwrap();
                        bar.wait();

                        conn.execute_batch("BEGIN").unwrap();
                        #[allow(clippy::cast_possible_wrap)]
                        let base = tid as i64 * CONCURRENT_RANGE_SIZE;
                        let mut stmt = conn
                            .prepare("INSERT INTO bench VALUES (?1, ('t' || ?1), (?1 * 7))")
                            .unwrap();
                        #[allow(clippy::cast_possible_wrap)]
                        for i in 0..CONCURRENT_ROWS_PER_THREAD as i64 {
                            stmt.execute(rusqlite::params![base + i]).unwrap();
                        }
                        conn.execute_batch("COMMIT").unwrap();
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
        });

        // FrankenSQLite: sequential MVCC on one connection.
        let fs = measure(&format!("fs_concurrent_{n_threads}t"), total_rows, || {
            let conn = fsqlite::Connection::open(":memory:").unwrap();
            apply_pragmas_fsqlite(&conn);
            conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
                .unwrap();

            for tid in 0..n_threads {
                conn.execute("BEGIN").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                let base = tid as i64 * CONCURRENT_RANGE_SIZE;
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..CONCURRENT_ROWS_PER_THREAD as i64 {
                    let id = base + i;
                    conn.execute(&format!(
                        "INSERT INTO bench VALUES ({id}, 't{id}', {})",
                        id * 7,
                    ))
                    .unwrap();
                }
                conn.execute("COMMIT").unwrap();
            }
        });

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{n_threads} writers x {CONCURRENT_ROWS_PER_THREAD} rows"),
            Some(cs),
            Some(fs),
        );
    }

    // Also benchmark C SQLite single-threaded for the same total work (baseline).
    let section = report.add_section(
        "Concurrent Writers — C SQLite Single-Thread Baseline",
        "Same total row count as concurrent tests, but single-threaded file-backed C SQLite.",
    );

    for &n_threads in CONCURRENT_THREAD_COUNTS {
        let total_rows = n_threads * CONCURRENT_ROWS_PER_THREAD;
        eprint!("  Benchmarking C SQLite single-thread baseline ({total_rows} rows)... ");

        let cs_single = measure(&format!("cs_single_{n_threads}t_equiv"), total_rows, || {
            let tmp = tempfile::NamedTempFile::new().unwrap();
            let path = tmp.path().to_str().unwrap().to_owned();
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "PRAGMA page_size = 4096;\
                     PRAGMA journal_mode = WAL;\
                     PRAGMA synchronous = NORMAL;\
                     PRAGMA cache_size = -64000;\
                     CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
            )
            .unwrap();

            conn.execute_batch("BEGIN").unwrap();
            let mut stmt = conn
                .prepare("INSERT INTO bench VALUES (?1, ('t' || ?1), (?1 * 7))")
                .unwrap();
            #[allow(clippy::cast_possible_wrap)]
            for i in 0..total_rows as i64 {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
            conn.execute_batch("COMMIT").unwrap();
        });

        eprintln!("C_single={}", format_duration(cs_single.median()));
        section.add_row(
            &format!("C SQLite 1 thread / {total_rows} rows (baseline)"),
            Some(cs_single),
            None,
        );
    }
}

// ─── Section 5: Read-after-write performance ───────────────────────────

fn bench_read_after_write(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "Read-After-Write Query Performance",
        "Insert N rows, then benchmark various SELECT patterns. Record: small_3col.",
    );

    let record_size = RecordSize::Small;

    for &count in row_counts {
        // Skip very large for query benchmarks.
        if count > 100_000 {
            continue;
        }

        eprint!("  Setting up {count} rows for read benchmarks... ");

        // Set up C SQLite.
        let cs_conn = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            apply_pragmas_csqlite(&conn);
            let create_sql = record_size.create_table_sql();
            conn.execute_batch(&format!("{create_sql};")).unwrap();
            conn.execute_batch("BEGIN").unwrap();
            {
                let mut stmt = conn.prepare(record_size.insert_sql_csqlite()).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
            }
            conn.execute_batch("COMMIT").unwrap();
            // Create secondary index.
            conn.execute_batch("CREATE INDEX idx_name ON bench(name);")
                .unwrap();
            conn
        };

        // Set up FrankenSQLite.
        let fs_conn = {
            let conn = fsqlite::Connection::open(":memory:").unwrap();
            apply_pragmas_fsqlite(&conn);
            conn.execute(record_size.create_table_sql()).unwrap();
            conn.execute("BEGIN").unwrap();
            #[allow(clippy::cast_possible_wrap)]
            for i in 0..count as i64 {
                conn.execute(&record_size.insert_sql_fsqlite(i)).unwrap();
            }
            conn.execute("COMMIT").unwrap();
            conn.execute("CREATE INDEX idx_name ON bench(name)")
                .unwrap();
            conn
        };

        eprintln!("done.");

        // Full table scan.
        eprint!("    Full table scan... ");
        let cs = {
            let mut stmt = cs_conn.prepare("SELECT * FROM bench").unwrap();
            measure(&format!("cs_scan_{count}"), count, || {
                let _rows: Vec<(i64,)> = stmt
                    .query_map([], |row| Ok((row.get(0).unwrap(),)))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            })
        };
        let fs = measure(&format!("fs_scan_{count}"), count, || {
            let _rows = fs_conn.query("SELECT * FROM bench").unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / full table scan"),
            Some(cs),
            Some(fs),
        );

        // Point lookup by PK.
        eprint!("    Point lookup (PK)... ");
        let target_id = (count / 2) as i64;
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT * FROM bench WHERE id = ?1")
                .unwrap();
            measure(&format!("cs_pk_{count}"), 1, || {
                let _: (i64,) = stmt
                    .query_row(rusqlite::params![target_id], |r| Ok((r.get(0).unwrap(),)))
                    .unwrap();
            })
        };
        let fs = measure(&format!("fs_pk_{count}"), 1, || {
            let _rows = fs_conn
                .query(&format!("SELECT * FROM bench WHERE id = {target_id}"))
                .unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / point lookup (PK)"),
            Some(cs),
            Some(fs),
        );

        // Range scan (10% of table).
        let range_size = count / 10;
        let range_start = (count / 4) as i64;
        #[allow(clippy::cast_possible_wrap)]
        let range_end = range_start + range_size as i64;
        eprint!("    Range scan ({range_size} rows)... ");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT * FROM bench WHERE id >= ?1 AND id < ?2")
                .unwrap();
            measure(&format!("cs_range_{count}"), range_size, || {
                let _rows: Vec<(i64,)> = stmt
                    .query_map(rusqlite::params![range_start, range_end], |row| {
                        Ok((row.get(0).unwrap(),))
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            })
        };
        let fs = measure(&format!("fs_range_{count}"), range_size, || {
            let _rows = fs_conn
                .query(&format!(
                    "SELECT * FROM bench WHERE id >= {range_start} AND id < {range_end}"
                ))
                .unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / range scan ({range_size} rows)"),
            Some(cs),
            Some(fs),
        );

        // COUNT(*)
        eprint!("    COUNT(*)... ");
        let cs = {
            let mut stmt = cs_conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
            measure(&format!("cs_count_{count}"), 1, || {
                let _: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
            })
        };
        let fs = measure(&format!("fs_count_{count}"), 1, || {
            let _rows = fs_conn.query("SELECT COUNT(*) FROM bench").unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(&format!("{count} rows / COUNT(*)"), Some(cs), Some(fs));

        // Aggregate SUM + GROUP BY.
        eprint!("    SUM + GROUP BY... ");
        let cs = {
            // Group by integer division to get ~10 groups.
            #[allow(clippy::cast_possible_wrap)]
            let group_divisor = (count / 10).max(1) as i64;
            let sql = format!(
                "SELECT (id / {group_divisor}), SUM(value) FROM bench GROUP BY (id / {group_divisor})"
            );
            let mut stmt = cs_conn.prepare(&sql).unwrap();
            measure(&format!("cs_groupby_{count}"), count, || {
                let _rows: Vec<(i64,)> = stmt
                    .query_map([], |row| Ok((row.get(0).unwrap(),)))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            })
        };
        let fs = {
            #[allow(clippy::cast_possible_wrap)]
            let group_divisor = (count / 10).max(1) as i64;
            let sql = format!(
                "SELECT (id / {group_divisor}), SUM(value) FROM bench GROUP BY (id / {group_divisor})"
            );
            measure(&format!("fs_groupby_{count}"), count, || {
                let _rows = fs_conn.query(&sql).unwrap();
            })
        };
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / SUM + GROUP BY (~10 groups)"),
            Some(cs),
            Some(fs),
        );

        // Indexed lookup on secondary index.
        eprint!("    Indexed lookup (secondary)... ");
        let target_name = format!("user_{target_id}");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT * FROM bench WHERE name = ?1")
                .unwrap();
            measure(&format!("cs_idx_{count}"), 1, || {
                let _rows: Vec<(i64,)> = stmt
                    .query_map(rusqlite::params![target_name], |row| {
                        Ok((row.get(0).unwrap(),))
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            })
        };
        let fs = measure(&format!("fs_idx_{count}"), 1, || {
            let _rows = fs_conn
                .query(&format!("SELECT * FROM bench WHERE name = '{target_name}'"))
                .unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / indexed lookup (secondary)"),
            Some(cs),
            Some(fs),
        );

        // ORDER BY + LIMIT.
        eprint!("    ORDER BY + LIMIT 20... ");
        let cs = {
            let mut stmt = cs_conn
                .prepare("SELECT * FROM bench ORDER BY value DESC LIMIT 20")
                .unwrap();
            measure(&format!("cs_order_{count}"), 20, || {
                let _rows: Vec<(i64,)> = stmt
                    .query_map([], |row| Ok((row.get(0).unwrap(),)))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            })
        };
        let fs = measure(&format!("fs_order_{count}"), 20, || {
            let _rows = fs_conn
                .query("SELECT * FROM bench ORDER BY value DESC LIMIT 20")
                .unwrap();
        });
        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / ORDER BY + LIMIT 20"),
            Some(cs),
            Some(fs),
        );
    }
}

// ─── Section 6: Update and delete throughput ───────────────────────────

fn bench_update_delete(report: &mut BenchReport, row_counts: &[usize]) {
    let section = report.add_section(
        "UPDATE/DELETEThroughput",
        "Pre-populated table with N rows. Measures batch update (10% of rows) and batch delete (5% of rows).",
    );

    let record_size = RecordSize::Small;

    for &count in row_counts {
        if count > 100_000 {
            continue;
        }

        let update_count = count / 10;
        let delete_count = count / 20;

        // Batch update: update 10% of rows.
        eprint!("  Benchmarking update {update_count}/{count} rows... ");

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("cs_update_{count}"), update_count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                conn.execute_batch("BEGIN").unwrap();
                let mut ins = conn.prepare(insert_sql).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    ins.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();

                conn.execute_batch("BEGIN").unwrap();
                let mut upd = conn
                    .prepare("UPDATE bench SET value = ?2 WHERE id = ?1")
                    .unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..update_count as i64 {
                    let id = i * 10; // Every 10th row.
                    upd.execute(rusqlite::params![id, 999.99]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            })
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fs_update_{count}"), update_count, || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(create_sql).unwrap();
                conn.execute("BEGIN").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    conn.execute(&record_size.insert_sql_fsqlite(i)).unwrap();
                }
                conn.execute("COMMIT").unwrap();

                conn.execute("BEGIN").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..update_count as i64 {
                    let id = i * 10;
                    conn.execute(&format!("UPDATE bench SET value = 999.99 WHERE id = {id}"))
                        .unwrap();
                }
                conn.execute("COMMIT").unwrap();
            })
        };

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / update {update_count} rows"),
            Some(cs),
            Some(fs),
        );

        // Batch delete: delete 5% of rows.
        eprint!("  Benchmarking delete {delete_count}/{count} rows... ");

        let cs = {
            let insert_sql = record_size.insert_sql_csqlite();
            let create_sql = record_size.create_table_sql();
            measure(&format!("cs_delete_{count}"), delete_count, || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(&format!("{create_sql};")).unwrap();
                conn.execute_batch("BEGIN").unwrap();
                let mut ins = conn.prepare(insert_sql).unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    ins.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();

                conn.execute_batch("BEGIN").unwrap();
                let mut del = conn.prepare("DELETE FROM bench WHERE id = ?1").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..delete_count as i64 {
                    let id = i * 20; // Every 20th row.
                    del.execute(rusqlite::params![id]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            })
        };

        let fs = {
            let create_sql = record_size.create_table_sql();
            measure(&format!("fs_delete_{count}"), delete_count, || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(create_sql).unwrap();
                conn.execute("BEGIN").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..count as i64 {
                    conn.execute(&record_size.insert_sql_fsqlite(i)).unwrap();
                }
                conn.execute("COMMIT").unwrap();

                conn.execute("BEGIN").unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..delete_count as i64 {
                    let id = i * 20;
                    conn.execute(&format!("DELETE FROM bench WHERE id = {id}"))
                        .unwrap();
                }
                conn.execute("COMMIT").unwrap();
            })
        };

        eprintln!(
            "C={} F={}",
            format_duration(cs.median()),
            format_duration(fs.median())
        );
        section.add_row(
            &format!("{count} rows / delete {delete_count} rows"),
            Some(cs),
            Some(fs),
        );
    }
}

// ─── Section 7: Mixed OLTP workload at scale ───────────────────────────

struct Rng64 {
    state: u64,
}

impl Rng64 {
    const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[allow(clippy::cast_possible_truncation)]
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % (bound as u64)) as usize
    }
}

fn bench_mixed_oltp(report: &mut BenchReport) {
    let section = report.add_section(
        "Mixed OLTP Workload (80% read / 20% write)",
        "Pre-seeded with 5K rows. Runs 5K operations with realistic distribution: \
         40% point lookups, 20% range scans, 20% aggregates, 15% inserts, 3% updates, 2% deletes.",
    );

    let ops = 5_000_usize;
    let seed_rows = 5_000_usize;

    eprint!("  Benchmarking mixed OLTP C SQLite... ");

    let cs = measure("cs_oltp", ops, || {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        apply_pragmas_csqlite(&conn);
        conn.execute_batch(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
        )
        .unwrap();
        conn.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = conn
                .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
                .unwrap();
            #[allow(clippy::cast_possible_wrap)]
            for i in 1..=seed_rows as i64 {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
        }
        conn.execute_batch("COMMIT").unwrap();

        let mut rng = Rng64::new(42);
        #[allow(clippy::cast_possible_wrap)]
        let mut next_id = seed_rows as i64 + 1;
        let mut select_pt = conn.prepare("SELECT * FROM bench WHERE id = ?1").unwrap();
        let mut select_range = conn
            .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
            .unwrap();
        let mut select_agg = conn
            .prepare("SELECT COUNT(*), SUM(score) FROM bench")
            .unwrap();
        let mut insert = conn
            .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
            .unwrap();
        let mut update = conn
            .prepare("UPDATE bench SET score = ?2 WHERE id = ?1")
            .unwrap();
        let mut delete = conn.prepare("DELETE FROM bench WHERE id = ?1").unwrap();

        #[allow(clippy::cast_possible_wrap)]
        for _ in 0..ops {
            let roll = rng.next_usize(100);
            if roll < 40 {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                let _ = select_pt.query_row(rusqlite::params![id], |_| Ok(()));
            } else if roll < 60 {
                let start = (rng.next_usize(seed_rows.saturating_sub(50)) + 1) as i64;
                let _: i64 = select_range
                    .query_row(rusqlite::params![start, start + 50], |r| r.get(0))
                    .unwrap_or(0);
            } else if roll < 80 {
                let _: (i64, i64) = select_agg
                    .query_row([], |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
                    .unwrap();
            } else if roll < 95 {
                let _ = insert.execute(rusqlite::params![next_id]);
                next_id += 1;
            } else if roll < 98 {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                let _ = update.execute(rusqlite::params![id, id * 99]);
            } else {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                let _ = delete.execute(rusqlite::params![id]);
            }
        }
    });

    eprintln!("C={}", format_duration(cs.median()));

    eprint!("  Benchmarking mixed OLTP FrankenSQLite... ");

    let fs = measure("fs_oltp", ops, || {
        let conn = fsqlite::Connection::open(":memory:").unwrap();
        apply_pragmas_fsqlite(&conn);
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        #[allow(clippy::cast_possible_wrap)]
        for i in 1..=seed_rows as i64 {
            conn.execute(&format!(
                "INSERT INTO bench VALUES ({i}, 'name_{i}', {})",
                i * 7,
            ))
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();

        let mut rng = Rng64::new(42);
        #[allow(clippy::cast_possible_wrap)]
        let mut next_id = seed_rows as i64 + 1;

        #[allow(clippy::cast_possible_wrap)]
        for _ in 0..ops {
            let roll = rng.next_usize(100);
            if roll < 40 {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                let _ = conn.query(&format!("SELECT * FROM bench WHERE id = {id}"));
            } else if roll < 60 {
                let start = (rng.next_usize(seed_rows.saturating_sub(50)) + 1) as i64;
                let _ = conn.query(&format!(
                    "SELECT COUNT(*) FROM bench WHERE id >= {start} AND id < {}",
                    start + 50,
                ));
            } else if roll < 80 {
                let _ = conn.query("SELECT COUNT(*), SUM(score) FROM bench");
            } else if roll < 95 {
                let _ = conn.execute(&format!(
                    "INSERT INTO bench VALUES ({next_id}, 'name_{next_id}', {})",
                    next_id * 7,
                ));
                next_id += 1;
            } else if roll < 98 {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                let _ = conn.execute(&format!(
                    "UPDATE bench SET score = {} WHERE id = {id}",
                    id * 99,
                ));
            } else {
                let id = (rng.next_usize(seed_rows) + 1) as i64;
                let _ = conn.execute(&format!("DELETE FROM bench WHERE id = {id}"));
            }
        }
    });

    eprintln!("F={}", format_duration(fs.median()));

    section.add_row("5K ops (80r/20w) on 5K-row table", Some(cs), Some(fs));
}

// ─── Main ──────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let quick = args.iter().any(|a| a == "--quick");
    let filter = args
        .windows(2)
        .find(|w| w[0] == "--filter")
        .map(|w| w[1].clone());
    let html_path = args
        .windows(2)
        .find(|w| w[0] == "--html")
        .map(|w| w[1].clone());

    let row_counts = if quick { ROW_COUNTS_QUICK } else { ROW_COUNTS };

    let should_run = |name: &str| -> bool {
        match &filter {
            Some(f) => name.to_lowercase().contains(&f.to_lowercase()),
            None => true,
        }
    };

    println!("\nComprehensive FrankenSQLite vs C SQLite Benchmark");
    println!("Mode: {}", if quick { "quick" } else { "full" });
    if let Some(ref f) = filter {
        println!("Filter: {f}");
    }
    println!();

    let mut report = BenchReport::new();

    // Section 1: Insert by row count across record sizes.
    if should_run("insert") {
        for &record_size in RecordSize::ALL {
            bench_insert_by_row_count(&mut report, row_counts, record_size);
        }
    }

    // Section 2: Transaction strategy comparison.
    if should_run("txn") || should_run("transaction") || should_run("insert") {
        bench_insert_by_txn_strategy(&mut report, row_counts);
    }

    // Section 3: Record size comparison.
    if should_run("record") || should_run("size") || should_run("insert") {
        bench_insert_by_record_size(&mut report);
    }

    // Section 4: Concurrent writers.
    if should_run("concurrent") || should_run("writer") {
        bench_concurrent_writers(&mut report);
    }

    // Section 5: Read-after-write.
    if should_run("read") || should_run("query") || should_run("select") {
        bench_read_after_write(&mut report, row_counts);
    }

    // Section 6: Update/delete.
    if should_run("update") || should_run("delete") {
        bench_update_delete(&mut report, row_counts);
    }

    // Section 7: Mixed OLTP.
    if should_run("oltp") || should_run("mixed") {
        bench_mixed_oltp(&mut report);
    }

    report.print();

    if let Some(ref path) = html_path {
        report.write_html(path);
    } else {
        // Default: write HTML alongside the text report.
        report.write_html("benchmark_report.html");
    }
}
