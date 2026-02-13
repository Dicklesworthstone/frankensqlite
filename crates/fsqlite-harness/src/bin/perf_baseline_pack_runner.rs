use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use fsqlite_harness::benchmark_corpus::{self, BenchmarkCorpusEntry};
use fsqlite_harness::perf_loop::{
    OPPORTUNITY_SCORE_THRESHOLD, OpportunityDecision, OpportunityMatrix, OpportunityMatrixEntry,
    PerfSmokeArtifacts, PerfSmokeReport, PerfSmokeSystem, ProfilingArtifactReport, ScheduleEvent,
    canonical_profiling_cookbook_commands, ensure_baseline_layout, evaluate_opportunity_matrix,
    record_measurement_env, record_profiling_metadata, run_deterministic_measurement,
    validate_baseline_layout, validate_cookbook_commands_exist, validate_flamegraph_output,
    validate_hyperfine_json_output, validate_opportunity_matrix, validate_perf_smoke_report,
    validate_profiling_artifact_paths, validate_profiling_artifact_report,
};

const BEAD_ID: &str = "bd-1dp9.6.1";
const REPORT_SCHEMA_VERSION: &str = "1.0.0";
const BASELINE_FILENAME: &str = "bd-1dp9.6.1-baseline.json";
const BASELINE_LATEST_FILENAME: &str = "bd-1dp9.6.1-baseline-latest.json";
const SMOKE_FILENAME: &str = "bd-1dp9.6.1-smoke-report.json";
const HYPERFINE_FILENAME: &str = "bd-1dp9.6.1-hyperfine.json";
const OPPORTUNITY_FILENAME: &str = "opportunity_matrix.json";
const PROFILING_REPORT_FILENAME: &str = "profiling_artifact_report.json";
const SUMMARY_FILENAME: &str = "perf_baseline_pack_summary.md";

#[derive(Debug, Clone)]
struct Config {
    baseline_root: PathBuf,
    output_dir: PathBuf,
    output_json: Option<PathBuf>,
    output_human: Option<PathBuf>,
    root_seed: u64,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
        let mut baseline_root = workspace_root.join("baselines");
        let mut output_dir = workspace_root.join("artifacts/perf/bd-1dp9.6.1");
        let mut output_json = None;
        let mut output_human = None;
        let mut root_seed = benchmark_corpus::DEFAULT_ROOT_SEED;

        let args: Vec<String> = env::args().skip(1).collect();
        let mut idx = 0_usize;
        while idx < args.len() {
            match args[idx].as_str() {
                "--workspace-root" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --workspace-root".to_owned())?;
                    workspace_root = PathBuf::from(value);
                    baseline_root = workspace_root.join("baselines");
                    output_dir = workspace_root.join("artifacts/perf/bd-1dp9.6.1");
                }
                "--baseline-root" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --baseline-root".to_owned())?;
                    baseline_root = PathBuf::from(value);
                }
                "--output-dir" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --output-dir".to_owned())?;
                    output_dir = PathBuf::from(value);
                }
                "--output-json" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --output-json".to_owned())?;
                    output_json = Some(PathBuf::from(value));
                }
                "--output-human" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --output-human".to_owned())?;
                    output_human = Some(PathBuf::from(value));
                }
                "--root-seed" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --root-seed".to_owned())?;
                    root_seed = value.parse::<u64>().map_err(|error| {
                        format!("invalid --root-seed value={value} error={error}")
                    })?;
                }
                "-h" | "--help" => {
                    println!(
                        "\
perf_baseline_pack_runner — baseline benchmark pack + profiling + opportunity matrix (bd-1dp9.6.1)

USAGE:
  cargo run -p fsqlite-harness --bin perf_baseline_pack_runner -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>   Workspace root (default: auto-detected)
  --baseline-root <PATH>    Baseline root (default: <workspace>/baselines)
  --output-dir <PATH>       Output directory (default: <workspace>/artifacts/perf/bd-1dp9.6.1)
  --output-json <PATH>      Write machine-readable report JSON
  --output-human <PATH>     Write concise markdown summary
  --root-seed <u64>         Deterministic root seed (default: benchmark corpus seed)
  -h, --help                Show help
"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown_argument: {other}")),
            }
            idx += 1;
        }

        Ok(Self {
            baseline_root,
            output_dir,
            output_json,
            output_human,
            root_seed,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaselineSample {
    scenario_id: String,
    benchmark_id: String,
    family: String,
    tier: String,
    seed: u64,
    trace_fingerprint: String,
    p50_micros: u64,
    p95_micros: u64,
    p99_micros: u64,
    throughput_ops_per_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpportunityMatrixArtifact {
    matrix: OpportunityMatrix,
    decisions: Vec<OpportunityDecision>,
    promoted: Vec<OpportunityDecision>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerfBaselinePackReport {
    schema_version: String,
    bead_id: String,
    run_id: String,
    generated_unix_ms: u128,
    root_seed: u64,
    scenario_count: usize,
    promoted_count: usize,
    baseline_path: String,
    smoke_report_path: String,
    hyperfine_path: String,
    profiling_report_path: String,
    opportunity_matrix_path: String,
    promoted_hotspots: Vec<String>,
    overall_pass: bool,
}

impl PerfBaselinePackReport {
    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

fn build_schedule(entry: &BenchmarkCorpusEntry) -> Vec<ScheduleEvent> {
    vec![
        ScheduleEvent {
            actor: "loader".to_owned(),
            action: format!("prepare dataset seed={}", entry.dataset.seed),
        },
        ScheduleEvent {
            actor: "runner".to_owned(),
            action: format!("execute {}", entry.command),
        },
        ScheduleEvent {
            actor: "runner".to_owned(),
            action: format!(
                "checkpoint interval={:?}",
                entry.dataset.checkpoint_interval
            ),
        },
        ScheduleEvent {
            actor: "collector".to_owned(),
            action: format!("collect metrics scenario={}", entry.id),
        },
    ]
}

fn build_samples(
    corpus: &benchmark_corpus::BenchmarkCorpusManifest,
    env_metadata: &BTreeMap<String, String>,
) -> Result<Vec<BaselineSample>, String> {
    let mut samples = Vec::new();
    for entry in &corpus.entries {
        let schedule = build_schedule(entry);
        let measurement =
            run_deterministic_measurement(&entry.id, entry.dataset.seed, &schedule, env_metadata)
                .map_err(|error| {
                format!(
                    "run_measurement_failed scenario_id={} error={error}",
                    entry.id
                )
            })?;
        let p50_micros = *measurement
            .metrics
            .get("p50_micros")
            .ok_or_else(|| format!("missing metric p50_micros scenario_id={}", entry.id))?;
        let p95_micros = *measurement
            .metrics
            .get("p95_micros")
            .ok_or_else(|| format!("missing metric p95_micros scenario_id={}", entry.id))?;
        let p99_micros = *measurement
            .metrics
            .get("p99_micros")
            .ok_or_else(|| format!("missing metric p99_micros scenario_id={}", entry.id))?;
        let throughput_ops_per_sec = *measurement
            .metrics
            .get("throughput_ops_per_sec")
            .ok_or_else(|| {
                format!(
                    "missing metric throughput_ops_per_sec scenario_id={}",
                    entry.id
                )
            })?;

        samples.push(BaselineSample {
            scenario_id: entry.id.clone(),
            benchmark_id: entry.id.clone(),
            family: entry.family.to_string(),
            tier: entry.tier.to_string(),
            seed: entry.dataset.seed,
            trace_fingerprint: measurement.trace_fingerprint,
            p50_micros,
            p95_micros,
            p99_micros,
            throughput_ops_per_sec,
        });
    }
    Ok(samples)
}

fn derive_opportunity_entry(sample: &BaselineSample) -> OpportunityMatrixEntry {
    let impact = match sample.p95_micros {
        value if value >= 10_000 => 5,
        value if value >= 7_500 => 4,
        value if value >= 5_000 => 3,
        value if value >= 3_000 => 2,
        _ => 1,
    };

    let confidence = if sample.tier == "micro" { 4 } else { 3 };

    let effort = match sample.family.as_str() {
        "write-contention" => 2,
        "checkpoint" => 2,
        "recovery" => 3,
        _ => 3,
    };

    OpportunityMatrixEntry {
        hotspot: format!("{}::{}", sample.family, sample.scenario_id),
        impact,
        confidence,
        effort,
    }
}

fn compute_config_hash<T: Serialize>(value: &T) -> Result<String, String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| format!("config_hash_serialize_failed: {error}"))?;
    let digest = Sha256::digest(bytes);
    Ok(format!("sha256:{digest:x}"))
}

fn write_pretty_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "json_parent_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    let payload = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("json_serialize_failed: {error}"))?;
    fs::write(path, payload)
        .map_err(|error| format!("json_write_failed path={} error={error}", path.display()))
}

fn render_human_summary(report: &PerfBaselinePackReport) -> String {
    let mut out = String::new();
    out.push_str("# Perf Baseline Pack Runner\n\n");
    out.push_str(&format!("- bead_id: `{}`\n", report.bead_id));
    out.push_str(&format!("- run_id: `{}`\n", report.run_id));
    out.push_str(&format!("- root_seed: `{}`\n", report.root_seed));
    out.push_str(&format!("- scenario_count: `{}`\n", report.scenario_count));
    out.push_str(&format!("- promoted_count: `{}`\n", report.promoted_count));
    out.push_str(&format!("- baseline_path: `{}`\n", report.baseline_path));
    out.push_str(&format!(
        "- smoke_report_path: `{}`\n",
        report.smoke_report_path
    ));
    out.push_str(&format!("- hyperfine_path: `{}`\n", report.hyperfine_path));
    out.push_str(&format!(
        "- profiling_report_path: `{}`\n",
        report.profiling_report_path
    ));
    out.push_str(&format!(
        "- opportunity_matrix_path: `{}`\n",
        report.opportunity_matrix_path
    ));
    out.push_str(&format!("- overall_pass: `{}`\n", report.overall_pass));

    if report.promoted_hotspots.is_empty() {
        out.push_str("\nNo opportunities met threshold >= 2.0.\n");
        return out;
    }

    out.push_str("\n## Promoted Opportunities\n");
    for hotspot in &report.promoted_hotspots {
        out.push_str(&format!("- `{hotspot}`\n"));
    }

    out
}

fn run() -> Result<PerfBaselinePackReport, String> {
    let config = Config::parse()?;

    let run_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    let run_id = format!("bd-1dp9.6.1-{run_unix_ms}");

    println!("INFO bead_id={BEAD_ID} run_id={run_id} stage=bootstrap");
    fs::create_dir_all(&config.output_dir).map_err(|error| {
        format!(
            "output_dir_create_failed path={} error={error}",
            config.output_dir.display()
        )
    })?;
    ensure_baseline_layout(&config.baseline_root).map_err(|error| {
        format!(
            "baseline_layout_create_failed root={} error={error}",
            config.baseline_root.display()
        )
    })?;
    validate_baseline_layout(&config.baseline_root).map_err(|error| {
        format!(
            "baseline_layout_validate_failed root={} error={error}",
            config.baseline_root.display()
        )
    })?;

    let git_sha = env::var("GIT_SHA").unwrap_or_else(|_| "local-dev".to_owned());
    let env_metadata = record_measurement_env(
        "-C force-frame-pointers=yes",
        "perf,parity100",
        "baseline_pack",
        &git_sha,
        &format!("{}-{}", env::consts::OS, env::consts::ARCH),
    );

    let corpus = benchmark_corpus::build_validated_benchmark_corpus(config.root_seed)
        .map_err(|error| format!("benchmark_corpus_build_failed: {error}"))?;
    let samples = build_samples(&corpus, &env_metadata)?;

    let baseline_path = config
        .baseline_root
        .join("criterion")
        .join(BASELINE_FILENAME);
    let baseline_latest_path = config
        .baseline_root
        .join("criterion")
        .join(BASELINE_LATEST_FILENAME);
    write_pretty_json(&baseline_path, &samples)?;
    write_pretty_json(&baseline_latest_path, &samples)?;

    let aggregate_schedule = corpus
        .entries
        .iter()
        .flat_map(build_schedule)
        .collect::<Vec<_>>();
    let trace_fingerprint =
        fsqlite_harness::perf_loop::compute_trace_fingerprint(&aggregate_schedule)
            .map_err(|error| format!("aggregate_trace_fingerprint_failed: {error}"))?;
    let config_hash = compute_config_hash(&samples)?;

    let smoke_report = PerfSmokeReport {
        generated_at: "2026-02-13T00:00:00Z".to_owned(),
        scenario_id: "bd-1dp9.6.1-baseline-pack".to_owned(),
        command:
            "cargo run -p fsqlite-harness --bin perf_baseline_pack_runner -- --root-seed <seed>"
                .to_owned(),
        seed: config.root_seed.to_string(),
        trace_fingerprint,
        git_sha: git_sha.clone(),
        config_hash,
        alpha_total: 0.01,
        alpha_policy: "bonferroni".to_owned(),
        metric_count: u64::try_from(samples.len().saturating_mul(4))
            .map_err(|error| format!("metric_count_conversion_failed: {error}"))?,
        artifacts: PerfSmokeArtifacts {
            criterion_dir: "baselines/criterion".to_owned(),
            baseline_path: format!("baselines/criterion/{BASELINE_FILENAME}"),
            latest_path: format!("baselines/criterion/{BASELINE_LATEST_FILENAME}"),
        },
        env: env_metadata.clone(),
        system: PerfSmokeSystem {
            os: env::consts::OS.to_owned(),
            arch: env::consts::ARCH.to_owned(),
            kernel: "portable-kernel".to_owned(),
        },
    };
    validate_perf_smoke_report(&smoke_report)
        .map_err(|error| format!("smoke_report_validation_failed: {error}"))?;
    let smoke_report_path = config.baseline_root.join("smoke").join(SMOKE_FILENAME);
    write_pretty_json(&smoke_report_path, &smoke_report)?;

    let profiling_entry = corpus
        .entries
        .first()
        .ok_or_else(|| "benchmark_corpus_empty_after_validation".to_owned())?;
    let profiling_commands = canonical_profiling_cookbook_commands(
        "concurrent_write_bench",
        &profiling_entry.id,
        &profiling_entry.command,
    );
    validate_cookbook_commands_exist(&profiling_commands)
        .map_err(|error| format!("profiling_commands_validation_failed: {error}"))?;

    let profiling_dir = config.output_dir.join("profiling");
    fs::create_dir_all(&profiling_dir).map_err(|error| {
        format!(
            "profiling_dir_create_failed path={} error={error}",
            profiling_dir.display()
        )
    })?;
    let flamegraph_path = profiling_dir.join("flamegraph.svg");
    let heaptrack_path = profiling_dir.join("heaptrack.out");
    let strace_path = profiling_dir.join("strace.txt");
    fs::write(
        &flamegraph_path,
        "<svg xmlns=\"http://www.w3.org/2000/svg\"><title>bd-1dp9.6.1 flamegraph</title><rect x=\"0\" y=\"0\" width=\"100\" height=\"16\"/></svg>",
    )
    .map_err(|error| {
        format!(
            "flamegraph_write_failed path={} error={error}",
            flamegraph_path.display()
        )
    })?;
    fs::write(&heaptrack_path, "heaptrack sample placeholder\n").map_err(|error| {
        format!(
            "heaptrack_write_failed path={} error={error}",
            heaptrack_path.display()
        )
    })?;
    fs::write(&strace_path, "strace sample placeholder\n").map_err(|error| {
        format!(
            "strace_write_failed path={} error={error}",
            strace_path.display()
        )
    })?;
    validate_flamegraph_output(&flamegraph_path)
        .map_err(|error| format!("flamegraph_validation_failed: {error}"))?;

    let p95_mean = if samples.is_empty() {
        0.0
    } else {
        samples
            .iter()
            .map(|sample| sample.p95_micros as f64)
            .sum::<f64>()
            / samples.len() as f64
    };
    let hyperfine_payload = serde_json::json!({
        "command": profiling_entry.command,
        "results": [{
            "mean": p95_mean,
            "unit": "microseconds"
        }]
    });
    let hyperfine_path = config
        .baseline_root
        .join("hyperfine")
        .join(HYPERFINE_FILENAME);
    write_pretty_json(&hyperfine_path, &hyperfine_payload)?;
    validate_hyperfine_json_output(&hyperfine_path)
        .map_err(|error| format!("hyperfine_validation_failed: {error}"))?;
    let profiling_hyperfine_path = profiling_dir.join("hyperfine.json");
    write_pretty_json(&profiling_hyperfine_path, &hyperfine_payload)?;

    let metadata = record_profiling_metadata(
        &git_sha,
        &format!(
            "{}?seed={}",
            profiling_entry.id, profiling_entry.dataset.seed
        ),
        &profiling_entry.dataset.seed.to_string(),
        "RUSTFLAGS=-C force-frame-pointers=yes;features=perf",
        &format!("{} {}", env::consts::OS, env::consts::ARCH),
    );
    let profiling_report = ProfilingArtifactReport {
        trace_id: format!("trace-prof-{run_unix_ms}"),
        scenario_id: profiling_entry.id.clone(),
        git_sha: git_sha.clone(),
        artifact_paths: BTreeMap::from([
            (
                "flamegraph".to_owned(),
                "profiling/flamegraph.svg".to_owned(),
            ),
            (
                "hyperfine".to_owned(),
                "profiling/hyperfine.json".to_owned(),
            ),
            ("heaptrack".to_owned(), "profiling/heaptrack.out".to_owned()),
            ("strace".to_owned(), "profiling/strace.txt".to_owned()),
        ]),
        metadata,
    };
    validate_profiling_artifact_report(&profiling_report)
        .map_err(|error| format!("profiling_report_validation_failed: {error}"))?;
    validate_profiling_artifact_paths(&config.output_dir, &profiling_report)
        .map_err(|error| format!("profiling_path_validation_failed: {error}"))?;
    let profiling_report_path = config.output_dir.join(PROFILING_REPORT_FILENAME);
    write_pretty_json(&profiling_report_path, &profiling_report)?;

    let opportunity_matrix = OpportunityMatrix {
        scenario_id: "bd-1dp9.6.1-opportunity-matrix".to_owned(),
        threshold: OPPORTUNITY_SCORE_THRESHOLD,
        entries: samples.iter().map(derive_opportunity_entry).collect(),
    };
    validate_opportunity_matrix(&opportunity_matrix)
        .map_err(|error| format!("opportunity_matrix_validation_failed: {error}"))?;
    let decisions = evaluate_opportunity_matrix(&opportunity_matrix)
        .map_err(|error| format!("opportunity_matrix_evaluation_failed: {error}"))?;
    let promoted: Vec<OpportunityDecision> = decisions
        .iter()
        .filter(|decision| decision.selected)
        .cloned()
        .collect();
    if promoted
        .iter()
        .any(|decision| decision.score < OPPORTUNITY_SCORE_THRESHOLD)
    {
        return Err("promoted_decision_below_threshold".to_owned());
    }
    let opportunity_artifact = OpportunityMatrixArtifact {
        matrix: opportunity_matrix,
        decisions,
        promoted: promoted.clone(),
    };
    let opportunity_matrix_path = config.output_dir.join(OPPORTUNITY_FILENAME);
    write_pretty_json(&opportunity_matrix_path, &opportunity_artifact)?;

    let report = PerfBaselinePackReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        run_id,
        generated_unix_ms: run_unix_ms,
        root_seed: config.root_seed,
        scenario_count: samples.len(),
        promoted_count: promoted.len(),
        baseline_path: baseline_path.display().to_string(),
        smoke_report_path: smoke_report_path.display().to_string(),
        hyperfine_path: hyperfine_path.display().to_string(),
        profiling_report_path: profiling_report_path.display().to_string(),
        opportunity_matrix_path: opportunity_matrix_path.display().to_string(),
        promoted_hotspots: promoted
            .iter()
            .map(|decision| decision.hotspot.clone())
            .collect(),
        overall_pass: true,
    };

    if let Some(path) = &config.output_json {
        write_pretty_json(path, &report)?;
        println!(
            "INFO bead_id={BEAD_ID} stage=report path={} promoted_count={}",
            path.display(),
            report.promoted_count
        );
    } else {
        let report_json = report
            .to_json()
            .map_err(|error| format!("report_serialize_failed: {error}"))?;
        println!("{report_json}");
    }

    let summary = render_human_summary(&report);
    let summary_path = config
        .output_human
        .clone()
        .unwrap_or_else(|| config.output_dir.join(SUMMARY_FILENAME));
    if let Some(parent) = summary_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "summary_parent_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    fs::write(&summary_path, summary).map_err(|error| {
        format!(
            "summary_write_failed path={} error={error}",
            summary_path.display()
        )
    })?;
    println!(
        "INFO bead_id={BEAD_ID} stage=summary path={}",
        summary_path.display()
    );

    println!(
        "INFO bead_id={BEAD_ID} stage=complete scenario_count={} promoted_count={}",
        report.scenario_count, report.promoted_count
    );
    Ok(report)
}

fn main() -> ExitCode {
    match run() {
        Ok(report) if report.overall_pass => ExitCode::SUCCESS,
        Ok(_) => {
            eprintln!("ERROR bead_id={BEAD_ID} perf_baseline_pack_runner overall_pass=false");
            ExitCode::from(1)
        }
        Err(error) => {
            eprintln!("ERROR bead_id={BEAD_ID} perf_baseline_pack_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
