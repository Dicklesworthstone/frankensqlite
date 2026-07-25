//! `mt-read-bench` — multi-threaded read-heavy SELECT-by-rowid benchmark.
//!
//! Seeds a shared file-backed DB with N rows, then spawns T threads each
//! running M prepared `SELECT payload FROM bench WHERE id = ?1` probes
//! against a disjoint rowid range. Reports reads/sec at 1/2/4/8 threads for
//! FrankenSQLite vs rusqlite (C SQLite WAL).
//!
//! Motivation: bd-...'s pinned-read + rowid-prepared-lookup wins
//! (d9c410bb, 7e4a5409, 6438b35c, b86cd4e6) should carry the read path now
//! that `mt_mvcc_bench` has proven write-side parity. This bench isolates
//! reads so any remaining read-side gap is visible.
//!
//! Usage:
//!   cargo run --release -p fsqlite-e2e --bin mt-read-bench -- \
//!       [--rows=10000] [--reads-per-thread=50000] [--threads=1,2,4,8]
//!
//! Output: one row per thread count, pipe-separated, suitable for piping
//! into a markdown table or jq.

use serde::Serialize;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_ROWS: i64 = 10_000;
const DEFAULT_READS_PER_THREAD: usize = 50_000;
const DEFAULT_THREADS: &[usize] = &[1, 2, 4, 8];
const PAYLOAD_SIZE: usize = 64;
const ARTIFACT_SCHEMA_VERSION: &str = "fsqlite-e2e.mt_read_bench_artifact.v1";
const MANIFEST_SCHEMA_VERSION: &str = "fsqlite-e2e.mt_read_bench_manifest.v1";
const BEAD_ID: &str = "bd-db300.7.1.2";
const FIXTURE_ID: &str = "mt_read_generated";
const WORKLOAD_ID: &str = "select_by_rowid_read_heavy";
const SCENARIO_ID: &str = "mt-read-bench";
const MODE_FSQLITE_MVCC: &str = "fsqlite_mvcc";
const MODE_SQLITE_REFERENCE: &str = "sqlite_reference";

#[derive(Debug)]
struct BenchCli {
    rows: i64,
    reads_per_thread: usize,
    threads: Vec<usize>,
    artifact_dir: Option<PathBuf>,
    run_id: String,
    build_profile: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct EngineMeasurement {
    mode_id: &'static str,
    reads_total: u64,
    elapsed_ms: f64,
    reads_per_sec: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ThreadComparison {
    threads: usize,
    fsqlite: EngineMeasurement,
    sqlite_reference: EngineMeasurement,
    ratio: f64,
}

#[derive(Serialize)]
struct ArtifactConfig<'a> {
    rows: i64,
    reads_per_thread: usize,
    threads: &'a [usize],
}

#[derive(Serialize)]
struct AlignedCounterRow {
    row_id: String,
    fixture_id: &'static str,
    workload: &'static str,
    concurrency: usize,
    mode_id: &'static str,
    build_profile_id: String,
    source_revision: String,
    run_id: String,
    comparable: AlignedCounters,
    mode_specific: Vec<String>,
}

#[derive(Serialize)]
struct AlignedCounters {
    reads_total: u64,
    elapsed_ms: f64,
    reads_per_sec: f64,
}

#[derive(Serialize)]
struct ReadHeavyArtifact<'a> {
    schema_version: &'static str,
    bead_id: &'static str,
    run_id: &'a str,
    scenario_id: &'static str,
    generated_at_unix_ms: u128,
    config: ArtifactConfig<'a>,
    row_identity_fields: Vec<&'static str>,
    comparable_counter_ids: Vec<&'static str>,
    mode_specific_counter_ids: Vec<&'static str>,
    rows: Vec<AlignedCounterRow>,
    comparisons: &'a [ThreadComparison],
}

#[derive(Serialize)]
struct ArtifactManifest {
    schema_version: &'static str,
    bead_id: &'static str,
    run_id: String,
    scenario_id: &'static str,
    artifact_dir: String,
    replay_command: String,
    files: Vec<ArtifactFile>,
}

#[derive(Serialize)]
struct ArtifactFile {
    path: &'static str,
    description: &'static str,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = parse_cli()?;

    eprintln!(
        "mt-read-bench: rows={} reads_per_thread={} threads={:?}",
        cli.rows, cli.reads_per_thread, cli.threads
    );

    println!("threads | fs_rps       | sq_rps       | ratio");
    println!("--------|--------------|--------------|--------");
    let mut comparisons = Vec::new();
    for &t in &cli.threads {
        if t == 0 {
            continue;
        }
        let fs = run_fsqlite(t, cli.rows, cli.reads_per_thread);
        let sq = run_rusqlite(t, cli.rows, cli.reads_per_thread);
        #[allow(clippy::cast_precision_loss)]
        let ratio = fs.reads_per_sec / sq.reads_per_sec.max(1.0);
        println!(
            "{t:>7} | {fs:>12.0} | {sq:>12.0} | {ratio:>5.2}x",
            fs = fs.reads_per_sec,
            sq = sq.reads_per_sec
        );
        comparisons.push(ThreadComparison {
            threads: t,
            fsqlite: fs,
            sqlite_reference: sq,
            ratio,
        });
    }

    if let Some(artifact_dir) = cli.artifact_dir.as_ref() {
        write_artifacts(artifact_dir, &cli, &comparisons)?;
        eprintln!("artifact_dir={}", artifact_dir.display());
    }

    Ok(())
}

fn parse_cli() -> Result<BenchCli, Box<dyn Error>> {
    let mut rows = DEFAULT_ROWS;
    let mut reads_per_thread = DEFAULT_READS_PER_THREAD;
    let mut threads = DEFAULT_THREADS.to_vec();
    let mut artifact_dir = None;
    let mut run_id = default_run_id();
    let mut build_profile =
        std::env::var("FSQLITE_BENCH_BUILD_PROFILE").unwrap_or_else(|_| "unknown".to_owned());

    for arg in std::env::args().skip(1) {
        if let Some(v) = arg.strip_prefix("--rows=") {
            rows = v
                .parse()
                .map_err(|error| invalid_input(format!("invalid --rows value `{v}`: {error}")))?;
        } else if let Some(v) = arg.strip_prefix("--reads-per-thread=") {
            reads_per_thread = v.parse().map_err(|error| {
                invalid_input(format!("invalid --reads-per-thread value `{v}`: {error}"))
            })?;
        } else if let Some(v) = arg.strip_prefix("--threads=") {
            threads = parse_threads(v)?;
        } else if let Some(v) = arg.strip_prefix("--artifact-dir=") {
            artifact_dir = Some(PathBuf::from(v));
        } else if let Some(v) = arg.strip_prefix("--run-id=") {
            v.clone_into(&mut run_id);
        } else if let Some(v) = arg.strip_prefix("--build-profile=") {
            v.clone_into(&mut build_profile);
        } else {
            return Err(invalid_input(format!("unknown argument: {arg}")));
        }
    }

    if rows <= 0 {
        return Err(invalid_input("--rows must be positive"));
    }
    if reads_per_thread == 0 {
        return Err(invalid_input("--reads-per-thread must be positive"));
    }

    Ok(BenchCli {
        rows,
        reads_per_thread,
        threads,
        artifact_dir,
        run_id,
        build_profile,
    })
}

fn parse_threads(raw: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    let mut threads = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        let parsed = trimmed.parse().map_err(|error| {
            invalid_input(format!("invalid --threads entry `{trimmed}`: {error}"))
        })?;
        threads.push(parsed);
    }
    Ok(threads)
}

fn invalid_input(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

#[allow(clippy::cast_precision_loss)]
fn run_fsqlite(n_threads: usize, rows: i64, reads_per_thread: usize) -> EngineMeasurement {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path: String = tmp.path().to_string_lossy().into_owned();
    drop(tmp);

    // Seed
    {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path.clone()))
            .expect("fsqlite open seed");
        let _ = fsqlite_e2e::block_on(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;"));
        fsqlite_e2e::block_on(
            conn.execute("CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, payload TEXT)"),
        )
        .expect("create");
        fsqlite_e2e::block_on(conn.execute("BEGIN")).expect("begin");
        let stmt =
            fsqlite_e2e::block_on(conn.prepare("INSERT INTO bench (id, payload) VALUES (?1, ?2)"))
                .expect("prepare insert");
        let payload = "x".repeat(PAYLOAD_SIZE);
        for id in 1..=rows {
            let params = [
                fsqlite::SqliteValue::Integer(id),
                fsqlite::SqliteValue::Text(payload.clone().into()),
            ];
            fsqlite_e2e::block_on(stmt.execute_with_params(&params)).expect("insert");
        }
        fsqlite_e2e::block_on(conn.execute("COMMIT")).expect("commit");
    }

    let path = Arc::new(path);
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut handles = Vec::with_capacity(n_threads);
    let t0 = Instant::now();
    for tid in 0..n_threads {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path.as_str().to_owned()))
                .expect("fsqlite open");
            let _ = fsqlite_e2e::block_on(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;"));
            let stmt =
                fsqlite_e2e::block_on(conn.prepare("SELECT payload FROM bench WHERE id = ?1"))
                    .expect("prepare select");
            barrier.wait();
            let mut state = 0x0102_0304_0506_0708_u64 ^ (tid as u64).wrapping_mul(0x9e37);
            for _ in 0..reads_per_thread {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                #[allow(clippy::cast_possible_wrap)]
                let id = ((state % rows as u64) + 1) as i64;
                let params = [fsqlite::SqliteValue::Integer(id)];
                let _ = fsqlite_e2e::block_on(stmt.query_with_params(&params)).expect("query");
            }
        }));
    }
    for h in handles {
        h.join().expect("join");
    }
    measurement(MODE_FSQLITE_MVCC, n_threads, reads_per_thread, t0)
}

#[allow(clippy::cast_precision_loss)]
fn run_rusqlite(n_threads: usize, rows: i64, reads_per_thread: usize) -> EngineMeasurement {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path: String = tmp.path().to_string_lossy().into_owned();
    drop(tmp);

    // Seed
    {
        let conn = rusqlite::Connection::open(&path).expect("sqlite open seed");
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; \
             PRAGMA synchronous=NORMAL; \
             PRAGMA busy_timeout=5000; \
             CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, payload TEXT);",
        )
        .expect("pragmas");
        conn.execute_batch("BEGIN").expect("begin");
        let mut stmt = conn
            .prepare("INSERT INTO bench (id, payload) VALUES (?1, ?2)")
            .expect("prepare insert");
        let payload = "x".repeat(PAYLOAD_SIZE);
        for id in 1..=rows {
            stmt.execute(rusqlite::params![id, payload])
                .expect("insert");
        }
        drop(stmt);
        conn.execute_batch("COMMIT").expect("commit");
    }

    let path = Arc::new(path);
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut handles = Vec::with_capacity(n_threads);
    let t0 = Instant::now();
    for tid in 0..n_threads {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let conn = rusqlite::Connection::open(path.as_str()).expect("sqlite open");
            conn.execute_batch("PRAGMA busy_timeout=5000;").ok();
            let mut stmt = conn
                .prepare("SELECT payload FROM bench WHERE id = ?1")
                .expect("prepare select");
            barrier.wait();
            let mut state = 0x0102_0304_0506_0708_u64 ^ (tid as u64).wrapping_mul(0x9e37);
            for _ in 0..reads_per_thread {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                #[allow(clippy::cast_possible_wrap)]
                let id = ((state % rows as u64) + 1) as i64;
                let _: Option<String> = stmt.query_row(rusqlite::params![id], |r| r.get(0)).ok();
            }
        }));
    }
    for h in handles {
        h.join().expect("join");
    }
    measurement(MODE_SQLITE_REFERENCE, n_threads, reads_per_thread, t0)
}

#[allow(clippy::cast_precision_loss)]
fn measurement(
    mode_id: &'static str,
    n_threads: usize,
    reads_per_thread: usize,
    t0: Instant,
) -> EngineMeasurement {
    let elapsed = t0.elapsed().as_secs_f64();
    let reads_total = total_reads(n_threads, reads_per_thread);
    EngineMeasurement {
        mode_id,
        reads_total,
        elapsed_ms: elapsed * 1_000.0,
        reads_per_sec: reads_total as f64 / elapsed.max(f64::EPSILON),
    }
}

fn total_reads(n_threads: usize, reads_per_thread: usize) -> u64 {
    u64::try_from(n_threads)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(reads_per_thread).unwrap_or(u64::MAX))
}

fn default_run_id() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("mt-read-{seconds}")
}

fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn source_revision() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|revision| !revision.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn write_artifacts(
    artifact_dir: &Path,
    cli: &BenchCli,
    comparisons: &[ThreadComparison],
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(artifact_dir)?;

    let revision = source_revision();
    let artifact = ReadHeavyArtifact {
        schema_version: ARTIFACT_SCHEMA_VERSION,
        bead_id: BEAD_ID,
        run_id: &cli.run_id,
        scenario_id: SCENARIO_ID,
        generated_at_unix_ms: unix_time_ms(),
        config: ArtifactConfig {
            rows: cli.rows,
            reads_per_thread: cli.reads_per_thread,
            threads: &cli.threads,
        },
        row_identity_fields: vec![
            "fixture_id",
            "workload",
            "concurrency",
            "mode_id",
            "build_profile_id",
            "source_revision",
            "run_id",
        ],
        comparable_counter_ids: vec!["reads_total", "elapsed_ms", "reads_per_sec"],
        mode_specific_counter_ids: Vec::new(),
        rows: aligned_rows(comparisons, cli, &revision),
        comparisons,
    };

    fs::write(
        artifact_dir.join("results.json"),
        serde_json::to_vec_pretty(&artifact)?,
    )?;
    fs::write(
        artifact_dir.join("summary.md"),
        render_summary(cli, comparisons),
    )?;
    let manifest = ArtifactManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        bead_id: BEAD_ID,
        run_id: cli.run_id.clone(),
        scenario_id: SCENARIO_ID,
        artifact_dir: artifact_dir.display().to_string(),
        replay_command: replay_command(cli, artifact_dir),
        files: vec![
            ArtifactFile {
                path: "results.json",
                description: "Aligned read-heavy benchmark rows and counters",
            },
            ArtifactFile {
                path: "summary.md",
                description: "Human-readable read-heavy benchmark summary",
            },
            ArtifactFile {
                path: "manifest.json",
                description: "Artifact bundle manifest and replay command",
            },
        ],
    };
    fs::write(
        artifact_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    Ok(())
}

fn aligned_rows(
    comparisons: &[ThreadComparison],
    cli: &BenchCli,
    source_revision: &str,
) -> Vec<AlignedCounterRow> {
    let mut rows = Vec::with_capacity(comparisons.len() * 2);
    for comparison in comparisons {
        rows.push(aligned_row(
            comparison.threads,
            comparison.fsqlite,
            cli,
            source_revision,
        ));
        rows.push(aligned_row(
            comparison.threads,
            comparison.sqlite_reference,
            cli,
            source_revision,
        ));
    }
    rows
}

fn aligned_row(
    threads: usize,
    measurement: EngineMeasurement,
    cli: &BenchCli,
    source_revision: &str,
) -> AlignedCounterRow {
    AlignedCounterRow {
        row_id: format!("{WORKLOAD_ID}_c{threads}_{}", measurement.mode_id),
        fixture_id: FIXTURE_ID,
        workload: WORKLOAD_ID,
        concurrency: threads,
        mode_id: measurement.mode_id,
        build_profile_id: cli.build_profile.clone(),
        source_revision: source_revision.to_owned(),
        run_id: cli.run_id.clone(),
        comparable: AlignedCounters {
            reads_total: measurement.reads_total,
            elapsed_ms: measurement.elapsed_ms,
            reads_per_sec: measurement.reads_per_sec,
        },
        mode_specific: Vec::new(),
    }
}

fn render_summary(cli: &BenchCli, comparisons: &[ThreadComparison]) -> String {
    let mut out = format!(
        "# Read-Heavy Benchmark\n\n- Bead: `{BEAD_ID}`\n- Run: `{}`\n- Rows: `{}`\n- Reads/thread: `{}`\n- Build profile: `{}`\n\n| Threads | FrankenSQLite reads/sec | SQLite reads/sec | Ratio |\n|---:|---:|---:|---:|\n",
        cli.run_id, cli.rows, cli.reads_per_thread, cli.build_profile
    );
    for comparison in comparisons {
        out.push_str(&format!(
            "| {} | {:.0} | {:.0} | {:.2}x |\n",
            comparison.threads,
            comparison.fsqlite.reads_per_sec,
            comparison.sqlite_reference.reads_per_sec,
            comparison.ratio
        ));
    }
    out
}

fn replay_command(cli: &BenchCli, artifact_dir: &Path) -> String {
    format!(
        "rch exec -- env CARGO_TARGET_DIR=/data/tmp/rch_target_cod10_1777097212 \
         cargo run --profile release-perf -p fsqlite-e2e --bin mt-read-bench -- \
         --rows={} --reads-per-thread={} --threads={} --artifact-dir={} --run-id={} \
         --build-profile={}",
        cli.rows,
        cli.reads_per_thread,
        cli.threads
            .iter()
            .map(|thread_count| thread_count.to_string())
            .collect::<Vec<_>>()
            .join(","),
        artifact_dir.display(),
        cli.run_id,
        cli.build_profile
    )
}
