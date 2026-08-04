use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_e2e::benchmark::{BenchmarkSummary, IterationRecord, LatencyStats, ThroughputStats};
use fsqlite_e2e::fixture_select::{
    BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE, BenchmarkArtifactCommand, BenchmarkArtifactManifest,
    BenchmarkArtifactProvenanceCapture, BenchmarkArtifactRetentionClass,
    BenchmarkArtifactToolVersion, BenchmarkMode, ExpandedBenchmarkCell,
    PLACEMENT_PROFILE_BASELINE_UNPINNED, build_benchmark_artifact_manifest,
    load_beads_benchmark_campaign,
};
use fsqlite_e2e::methodology::{EnvironmentMeta, MethodologyMeta};
use fsqlite_e2e::overlay_honesty_gate::{
    MatrixRegressionThresholds, evaluate_matrix_regression_gate_from_paths,
    evaluate_overlay_honesty_gate_from_paths, load_benchmark_summaries,
};
use fsqlite_e2e::report_render::render_benchmark_summaries_markdown;
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Environment variables that can weaken or subset a matrix run.
///
/// A signed receipt binds `argv` but not environment, so any of these set
/// during a release run would silently change what was measured while the
/// receipt still validated. The release test therefore refuses to run when any
/// is present, rather than trusting that none were used.
///
/// Grouped by bypass class: fixture/workload/concurrency subsetting, duration
/// and iteration shortening, mode reduction, and comparator threshold
/// relaxation.
const RELEASE_FORBIDDEN_MATRIX_ENV: &[&str] = &[
    // Subsetting: silently measure fewer cells than the canonical matrix.
    "FSQLITE_MATRIX_DB",
    "FSQLITE_MATRIX_WORKLOAD",
    "FSQLITE_MATRIX_CONCURRENCY",
    // Duration/iteration: shorten the measurement until noise dominates.
    "FSQLITE_MATRIX_WARMUP",
    "FSQLITE_MATRIX_REPEAT",
    "FSQLITE_MATRIX_MIN_ITERS",
    "FSQLITE_MATRIX_TIME_SECS",
    // Mode: run something other than the full canonical matrix.
    "FSQLITE_MATRIX_MODE",
    // Thresholds: relax the comparator until any result passes.
    "FSQLITE_MATRIX_MAX_P95_RATIO",
    "FSQLITE_MATRIX_MIN_THROUGHPUT_RATIO",
    // Baseline: point the historical comparison at an arbitrary file, or omit
    // it and skip the comparison silently. Refused outright at v0.2.0 because
    // no canonical historical baseline exists to select.
    "FSQLITE_MATRIX_BASELINE_JSONL",
];

/// Manifest status recorded when no canonical historical baseline exists.
///
/// The v0.2.0 release is the bootstrap: it publishes the first immutable
/// complete-matrix baseline instead of comparing against one.
const MATRIX_REGRESSION_STATUS_BOOTSTRAP: &str = "unavailable_bootstrap_v0_2_0";

/// The single package version permitted to skip historical regression.
const MATRIX_BOOTSTRAP_PKG_VERSION: &str = "0.2.0";

/// Refuse the bootstrap escape at any version other than [`MATRIX_BOOTSTRAP_PKG_VERSION`].
///
/// Bootstrap is a one-time concession: v0.2.0 publishes the first immutable
/// complete-matrix baseline. Once that baseline exists, a later release that
/// still skipped historical regression would be silently unguarded, so this
/// fails closed and forces the successor to wire the real comparison.
fn bootstrap_allowed_for_version(pkg_version: &str) -> Result<(), String> {
    if pkg_version == MATRIX_BOOTSTRAP_PKG_VERSION {
        return Ok(());
    }
    Err(format!(
        "historical-regression bootstrap is permitted only at package version \
         {MATRIX_BOOTSTRAP_PKG_VERSION}, found `{pkg_version}`; v{MATRIX_BOOTSTRAP_PKG_VERSION} \
         published the first immutable baseline, so this release must compare against it"
    ))
}

/// Names from [`RELEASE_FORBIDDEN_MATRIX_ENV`] that `is_set` reports as present.
///
/// Pure: the caller supplies the lookup, so keepers can exercise every bypass
/// class without mutating process-global environment state.
fn forbidden_matrix_env_present(is_set: impl Fn(&str) -> bool) -> Vec<&'static str> {
    RELEASE_FORBIDDEN_MATRIX_ENV
        .iter()
        .copied()
        .filter(|name| is_set(name))
        .collect()
}

/// Root of the offline verifier's evidence namespace.
///
/// Mirrors `EVIDENCE_PATH_PREFIX` in the phase-five release guard, which
/// requires every evidence path to be canonically spelled and stored below this
/// prefix.
const RELEASE_EVIDENCE_PREFIX: &str = "tests/artifacts/release-evidence";

/// Suffix identifying the c1 overlay scorecard within one commit's evidence.
const RELEASE_C1_SCORECARD_SUFFIX: &str = "performance/c1/c1_scorecard.json";

/// Suffix identifying the persistent overlay scorecard within one commit's
/// evidence.
const RELEASE_PERSISTENT_SCORECARD_SUFFIX: &str =
    "performance/persistent/persistent_scorecard.json";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root should exist")
        .to_path_buf()
}

fn short_hash(value: &str) -> String {
    value.chars().take(12).collect()
}

fn default_matrix_artifact_dir(
    repo_root: &Path,
    campaign: &fsqlite_e2e::fixture_select::BeadsBenchmarkCampaign,
    run_id: &str,
    source_revision: &str,
    beads_data_hash: &str,
) -> PathBuf {
    repo_root
        .join(&campaign.artifact_contract.artifact_root_relpath)
        .join(format!(
            "matrix_suite__run_{run_id}__rev_{}__beads_{}",
            short_hash(source_revision),
            short_hash(beads_data_hash),
        ))
}

fn resolve_matrix_artifact_dir(
    repo_root: &Path,
    campaign: &fsqlite_e2e::fixture_select::BeadsBenchmarkCampaign,
    override_path: Option<PathBuf>,
    run_id: &str,
    source_revision: &str,
    beads_data_hash: &str,
) -> PathBuf {
    override_path.map_or_else(
        || {
            default_matrix_artifact_dir(
                repo_root,
                campaign,
                run_id,
                source_revision,
                beads_data_hash,
            )
        },
        |path| {
            if path.is_absolute() {
                path
            } else {
                repo_root.join(path)
            }
        },
    )
}

fn artifact_dir(
    repo_root: &Path,
    campaign: &fsqlite_e2e::fixture_select::BeadsBenchmarkCampaign,
    run_id: &str,
    source_revision: &str,
    beads_data_hash: &str,
) -> PathBuf {
    resolve_matrix_artifact_dir(
        repo_root,
        campaign,
        std::env::var_os("FSQLITE_MATRIX_ARTIFACT_DIR").map(PathBuf::from),
        run_id,
        source_revision,
        beads_data_hash,
    )
}

fn bench_binary() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_realdb-e2e").map_or_else(
        || panic!("CARGO_BIN_EXE_realdb-e2e should be available for integration tests"),
        PathBuf::from,
    )
}

fn print_progress(label: &str, line: &str) {
    if let Ok(summary) = serde_json::from_str::<serde_json::Value>(line) {
        let benchmark_id = summary["benchmark_id"].as_str().unwrap_or("unknown");
        let median_ops = summary["throughput"]["median_ops_per_sec"]
            .as_f64()
            .unwrap_or(0.0);
        let p95_ms = summary["latency"]["p95_ms"].as_f64().unwrap_or(0.0);
        let measurement_count = summary["measurement_count"].as_u64().unwrap_or(0);
        println!(
            "[bench:{label}] {benchmark_id} median_ops/s={median_ops:.1} p95_ms={p95_ms:.1} n={measurement_count}"
        );
    } else if !line.trim().is_empty() {
        println!("[bench:{label}] {line}");
    }
}

fn run_bench(label: &str, args: &[String]) -> Result<ExitStatus, Box<dyn Error>> {
    let mut child = Command::new(bench_binary())
        .current_dir(repo_root())
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or("bench child stdout should be piped")?;
    for line in BufReader::new(stdout).lines() {
        print_progress(label, &line?);
    }

    let status = child.wait()?;
    Ok(status)
}

fn run_command_capture_stdout(args: &[String]) -> Result<(ExitStatus, String), Box<dyn Error>> {
    let output = Command::new(bench_binary())
        .current_dir(repo_root())
        .args(args)
        .stderr(Stdio::inherit())
        .output()?;
    Ok((output.status, String::from_utf8(output.stdout)?))
}

fn assert_success(status: ExitStatus, label: &str) -> Result<(), Box<dyn Error>> {
    if status.success() {
        return Ok(());
    }
    Err(format!("{label} failed with status {:?}", status.code()).into())
}

fn apply_common_filters(args: &mut Vec<String>) {
    if let Ok(fixture_filter) = std::env::var("FSQLITE_MATRIX_DB") {
        args.push("--db".to_owned());
        args.push(fixture_filter);
    }
    if let Ok(workload_filter) = std::env::var("FSQLITE_MATRIX_WORKLOAD") {
        args.push("--preset".to_owned());
        args.push(workload_filter);
    }
    if let Ok(concurrency_filter) = std::env::var("FSQLITE_MATRIX_CONCURRENCY") {
        args.push("--concurrency".to_owned());
        args.push(concurrency_filter);
    }
    if let Ok(warmup) = std::env::var("FSQLITE_MATRIX_WARMUP") {
        args.push("--warmup".to_owned());
        args.push(warmup);
    }
    if let Ok(repeat) = std::env::var("FSQLITE_MATRIX_REPEAT") {
        args.push("--repeat".to_owned());
        args.push(repeat);
    }
    if let Ok(min_iters) = std::env::var("FSQLITE_MATRIX_MIN_ITERS") {
        args.push("--min-iters".to_owned());
        args.push(min_iters);
    }
    if let Ok(time_secs) = std::env::var("FSQLITE_MATRIX_TIME_SECS") {
        args.push("--time-secs".to_owned());
        args.push(time_secs);
    }
}

fn shell_escape(raw: &str) -> String {
    if raw
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"/._-*=+".contains(&byte))
    {
        return raw.to_owned();
    }
    let escaped = raw.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

fn format_command_line(args: &[String]) -> String {
    let mut rendered = vec![shell_escape(&bench_binary().display().to_string())];
    rendered.extend(args.iter().map(|arg| shell_escape(arg)));
    rendered.join(" ")
}

fn tool_version(tool: &str, args: &[&str]) -> String {
    Command::new(tool)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn benchmark_tool_versions() -> Vec<BenchmarkArtifactToolVersion> {
    let mut tool_versions = vec![
        BenchmarkArtifactToolVersion {
            tool: "cargo".to_owned(),
            version: tool_version("cargo", &["--version"]),
        },
        BenchmarkArtifactToolVersion {
            tool: "git".to_owned(),
            version: tool_version("git", &["--version"]),
        },
        BenchmarkArtifactToolVersion {
            tool: "rch".to_owned(),
            version: tool_version("rch", &["--version"]),
        },
        BenchmarkArtifactToolVersion {
            tool: "rustc".to_owned(),
            version: tool_version("rustc", &["--version"]),
        },
    ];
    tool_versions.sort_by(|left, right| left.tool.cmp(&right.tool));
    tool_versions
}

fn matrix_run_id() -> String {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("matrix-{now_ms}")
}

fn git_head_revision(repo_root: &Path) -> Result<String, Box<dyn Error>> {
    let output = Command::new("git")
        .args(["-C", &repo_root.display().to_string(), "rev-parse", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err("git rev-parse HEAD failed".into());
    }
    let revision = String::from_utf8(output.stdout)?;
    Ok(revision.trim().to_owned())
}

fn sha256_file(path: &Path) -> Result<String, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    Ok(fsqlite_e2e::bytes_to_lower_hex(Sha256::digest(bytes)))
}

fn benchmark_mode_from_engine(engine: &str) -> Result<BenchmarkMode, Box<dyn Error>> {
    match engine {
        "sqlite3" | "sqlite_reference" => Ok(BenchmarkMode::SqliteReference),
        "fsqlite_mvcc" | "fsqlite" => Ok(BenchmarkMode::FsqliteMvcc),
        "fsqlite_single_writer" => Ok(BenchmarkMode::FsqliteSingleWriter),
        other => Err(format!("unknown benchmark engine `{other}`").into()),
    }
}

fn benchmark_mode_id(mode: BenchmarkMode) -> &'static str {
    match mode {
        BenchmarkMode::SqliteReference => "sqlite_reference",
        BenchmarkMode::FsqliteMvcc => "fsqlite_mvcc",
        BenchmarkMode::FsqliteSingleWriter => "fsqlite_single_writer",
    }
}

fn hardware_signature(
    campaign: &fsqlite_e2e::fixture_select::BeadsBenchmarkCampaign,
    hardware_class_id: &str,
) -> Result<String, Box<dyn Error>> {
    let hardware_class = campaign
        .hardware_classes
        .iter()
        .find(|hardware| hardware.id == hardware_class_id)
        .ok_or_else(|| format!("unknown hardware class `{hardware_class_id}`"))?;
    Ok(format!(
        "{}:{}:{}",
        hardware_class.id_fields.os_family.as_str(),
        hardware_class.id_fields.cpu_arch.as_str(),
        hardware_class.id_fields.topology_class.as_str()
    ))
}

fn resolve_canonical_cell(
    campaign: &fsqlite_e2e::fixture_select::BeadsBenchmarkCampaign,
    summary: &BenchmarkSummary,
    mode: BenchmarkMode,
) -> Result<ExpandedBenchmarkCell, Box<dyn Error>> {
    let matching_rows = campaign
        .matrix_rows
        .iter()
        .filter(|row| {
            row.workload == summary.workload
                && row.concurrency == summary.concurrency
                && row
                    .fixtures
                    .iter()
                    .any(|fixture| fixture == &summary.fixture_id)
                && row.modes.contains(&mode)
        })
        .collect::<Vec<_>>();
    let row = match matching_rows.as_slice() {
        [] => {
            return Err(format!(
                "no canonical matrix row for fixture={} workload={} concurrency={} mode={}",
                summary.fixture_id,
                summary.workload,
                summary.concurrency,
                benchmark_mode_id(mode)
            )
            .into());
        }
        [row] => *row,
        rows => {
            let row_ids = rows
                .iter()
                .map(|row| row.row_id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "ambiguous canonical matrix rows for fixture={} workload={} concurrency={} mode={}: {row_ids}",
                summary.fixture_id,
                summary.workload,
                summary.concurrency,
                benchmark_mode_id(mode)
            )
            .into());
        }
    };
    let placement = row
        .placement_variants
        .iter()
        .find(|variant| variant.placement_profile_id == PLACEMENT_PROFILE_BASELINE_UNPINNED)
        .or_else(|| {
            row.placement_variants
                .iter()
                .find(|variant| variant.required)
        })
        .or_else(|| row.placement_variants.first())
        .ok_or_else(|| format!("row `{}` has no placement variants", row.row_id))?;
    Ok(ExpandedBenchmarkCell {
        row_id: row.row_id.clone(),
        fixture_id: summary.fixture_id.clone(),
        workload: summary.workload.clone(),
        concurrency: summary.concurrency,
        mode,
        placement_profile_id: placement.placement_profile_id.clone(),
        hardware_class_id: placement.hardware_class_id.clone(),
        retry_policy_id: row.retry_policy_id.clone(),
        build_profile_id: row.build_profile_id.clone(),
        seed_policy_id: row.seed_policy_id.clone(),
    })
}

fn validate_matrix_mode(mode: &str) -> Result<(), Box<dyn Error>> {
    const SUPPORTED_MODES: [&str; 5] = ["full", "both", "sqlite3", "mvcc", "single_writer"];
    if SUPPORTED_MODES.contains(&mode) {
        return Ok(());
    }
    Err(format!(
        "unsupported FSQLITE_MATRIX_MODE `{mode}`; expected one of: {}",
        SUPPORTED_MODES.join(", ")
    )
    .into())
}

#[test]
fn matrix_mode_validation_rejects_unknown_values() {
    for mode in ["full", "both", "sqlite3", "mvcc", "single_writer"] {
        validate_matrix_mode(mode).expect("supported matrix mode should validate");
    }
    assert!(
        validate_matrix_mode("bogus").is_err(),
        "unknown matrix modes must fail fast instead of silently doing nothing"
    );
}

#[test]
fn beads_campaign_row_keys_are_unambiguous() -> Result<(), Box<dyn Error>> {
    let campaign = load_beads_benchmark_campaign(&repo_root())
        .map_err(|error| format!("load canonical Beads benchmark campaign: {error}"))?;
    let mut keys: BTreeMap<(String, String, u16, String), Vec<String>> = BTreeMap::new();

    for row in &campaign.matrix_rows {
        for fixture_id in &row.fixtures {
            for mode in &row.modes {
                keys.entry((
                    fixture_id.clone(),
                    row.workload.clone(),
                    row.concurrency,
                    benchmark_mode_id(*mode).to_owned(),
                ))
                .or_default()
                .push(row.row_id.clone());
            }
        }
    }

    let ambiguous = keys
        .into_iter()
        .filter(|(_, row_ids)| row_ids.len() > 1)
        .map(|((fixture_id, workload, concurrency, mode_id), row_ids)| {
            format!(
                "fixture={fixture_id} workload={workload} concurrency={concurrency} mode={mode_id} rows={}",
                row_ids.join(", ")
            )
        })
        .collect::<Vec<_>>();
    assert!(
        ambiguous.is_empty(),
        "campaign row keys must stay unique for canonical resolution:\n{}",
        ambiguous.join("\n")
    );
    Ok(())
}

#[test]
fn matrix_artifact_dir_default_tracks_current_run_identity() -> Result<(), Box<dyn Error>> {
    let repo_root = repo_root();
    let campaign = load_beads_benchmark_campaign(&repo_root)
        .map_err(|error| format!("load canonical Beads benchmark campaign: {error}"))?;
    let beads_data_hash = "a".repeat(64);
    let artifact_dir = resolve_matrix_artifact_dir(
        &repo_root,
        &campaign,
        None,
        "matrix-123",
        "0123456789abcdef0123456789abcdef01234567",
        &beads_data_hash,
    );
    assert_eq!(
        artifact_dir,
        repo_root
            .join(&campaign.artifact_contract.artifact_root_relpath)
            .join("matrix_suite__run_matrix-123__rev_0123456789ab__beads_aaaaaaaaaaaa")
    );
    Ok(())
}

#[test]
fn matrix_artifact_dir_relative_override_is_repo_relative() -> Result<(), Box<dyn Error>> {
    let repo_root = repo_root();
    let campaign = load_beads_benchmark_campaign(&repo_root)
        .map_err(|error| format!("load canonical Beads benchmark campaign: {error}"))?;
    let artifact_dir = resolve_matrix_artifact_dir(
        &repo_root,
        &campaign,
        Some(PathBuf::from("tmp/matrix-output")),
        "matrix-123",
        "0123456789abcdef0123456789abcdef01234567",
        &"a".repeat(64),
    );
    assert_eq!(artifact_dir, repo_root.join("tmp/matrix-output"));
    Ok(())
}

fn sample_benchmark_summary(
    benchmark_id: &str,
    engine: &str,
    workload: &str,
    fixture_id: &str,
    concurrency: u16,
    p95_ms: f64,
    throughput_ops_per_sec: f64,
) -> BenchmarkSummary {
    BenchmarkSummary {
        benchmark_id: benchmark_id.to_owned(),
        engine: engine.to_owned(),
        workload: workload.to_owned(),
        fixture_id: fixture_id.to_owned(),
        concurrency,
        methodology: MethodologyMeta::current(),
        environment: EnvironmentMeta::capture("test"),
        warmup_count: 0,
        measurement_count: 3,
        total_measurement_ms: 30,
        latency: LatencyStats {
            min_ms: p95_ms * 0.5,
            max_ms: p95_ms * 1.1,
            mean_ms: p95_ms * 0.8,
            median_ms: p95_ms * 0.75,
            p95_ms,
            p99_ms: p95_ms * 1.05,
            stddev_ms: p95_ms * 0.1,
        },
        throughput: ThroughputStats {
            mean_ops_per_sec: throughput_ops_per_sec * 0.98,
            median_ops_per_sec: throughput_ops_per_sec,
            peak_ops_per_sec: throughput_ops_per_sec * 1.02,
        },
        comparison: None,
        aggregated_hot_path: None,
        iterations: vec![IterationRecord {
            iteration: 0,
            wall_time_ms: 10,
            ops_per_sec: throughput_ops_per_sec,
            ops_total: 100,
            retries: 0,
            aborts: 0,
            error: None,
        }],
    }
}

#[test]
fn matrix_regression_gate_loads_canonical_record_lines_via_benchmark_summary() {
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let path = tempdir.path().join("canonical.jsonl");
    let payload = serde_json::json!({
        "benchmark_id": "fsqlite:count_star:fixture:c1",
        "engine": "fsqlite",
        "workload": "count_star",
        "fixture_id": "fixture",
        "concurrency": 1,
        "methodology": MethodologyMeta::current(),
        "environment": EnvironmentMeta::capture("test"),
        "warmup_count": 0,
        "measurement_count": 1,
        "total_measurement_ms": 10,
        "latency": {
            "min_ms": 1.0,
            "max_ms": 2.0,
            "mean_ms": 1.5,
            "median_ms": 1.4,
            "p95_ms": 1.9,
            "p99_ms": 2.0,
            "stddev_ms": 0.1
        },
        "throughput": {
            "mean_ops_per_sec": 1000.0,
            "median_ops_per_sec": 950.0,
            "peak_ops_per_sec": 1100.0
        },
        "iterations": [],
        "row_id": "row-1",
        "mode_id": "fsqlite_mvcc"
    });
    fs::write(
        &path,
        format!(
            "{}\n",
            serde_json::to_string(&payload).expect("serialize payload")
        ),
    )
    .expect("write canonical jsonl");

    let summaries = load_benchmark_summaries(&path).expect("load summaries");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].benchmark_id, "fsqlite:count_star:fixture:c1");
}

#[test]
fn matrix_regression_gate_flags_ratio_failures_and_missing_baselines() {
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let baseline_path = tempdir.path().join("baseline.jsonl");
    let current_path = tempdir.path().join("current.jsonl");

    let baseline = [
        sample_benchmark_summary(
            "fsqlite:count_star:fixture:c1",
            "fsqlite",
            "count_star",
            "fixture",
            1,
            10.0,
            1_000.0,
        ),
        sample_benchmark_summary(
            "fsqlite:exists_subquery:fixture:c1",
            "fsqlite",
            "exists_subquery",
            "fixture",
            1,
            20.0,
            800.0,
        ),
    ];
    let current = [
        sample_benchmark_summary(
            "fsqlite:count_star:fixture:c1",
            "fsqlite",
            "count_star",
            "fixture",
            1,
            13.0,
            700.0,
        ),
        sample_benchmark_summary(
            "fsqlite:missing_baseline:fixture:c1",
            "fsqlite",
            "missing_baseline",
            "fixture",
            1,
            5.0,
            500.0,
        ),
    ];

    fs::write(
        &baseline_path,
        format!(
            "{}\n{}\n",
            serde_json::to_string(&baseline[0]).expect("serialize baseline[0]"),
            serde_json::to_string(&baseline[1]).expect("serialize baseline[1]")
        ),
    )
    .expect("write baseline");
    fs::write(
        &current_path,
        format!(
            "{}\n{}\n",
            serde_json::to_string(&current[0]).expect("serialize current[0]"),
            serde_json::to_string(&current[1]).expect("serialize current[1]")
        ),
    )
    .expect("write current");

    let report = evaluate_matrix_regression_gate_from_paths(
        &baseline_path,
        &current_path,
        MatrixRegressionThresholds {
            max_p95_ratio: 1.20,
            min_throughput_ratio: 0.80,
        },
    )
    .expect("evaluate regression gate");

    assert_eq!(report.compared_cells, 1);
    assert_eq!(
        report.missing_baseline_cells,
        vec![fsqlite_e2e::overlay_honesty_gate::BenchmarkCellKey {
            benchmark_id: "fsqlite:missing_baseline:fixture:c1".to_owned(),
            row_id: None,
            fixture_id: "fixture".to_owned(),
            workload: "missing_baseline".to_owned(),
            concurrency: 1,
            mode_id: "fsqlite".to_owned(),
        }]
    );
    assert_eq!(
        report.failing_cells,
        vec![fsqlite_e2e::overlay_honesty_gate::BenchmarkCellKey {
            benchmark_id: "fsqlite:count_star:fixture:c1".to_owned(),
            row_id: None,
            fixture_id: "fixture".to_owned(),
            workload: "count_star".to_owned(),
            concurrency: 1,
            mode_id: "fsqlite".to_owned(),
        }]
    );
    assert!(
        report
            .failure_summary()
            .expect("gate should fail")
            .contains("regressed cells")
    );
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalBenchmarkRecord {
    #[serde(flatten)]
    summary: BenchmarkSummary,
    row_id: String,
    mode_id: String,
    retry_policy_id: String,
    seed_policy_id: String,
    build_profile_id: String,
    placement_profile_id: String,
    hardware_class_id: String,
    hardware_signature: String,
    source_revision: String,
    beads_data_hash: String,
    run_id: String,
    artifact_bundle_key: String,
    artifact_bundle_name: String,
    artifact_bundle_dir: String,
    artifact_bundle_relpath: String,
    artifact_manifest_path: String,
    campaign_manifest_path: String,
    canonical_artifact_manifest: BenchmarkArtifactManifest,
}

fn hardware_discovery_bundle_json(record: &CanonicalBenchmarkRecord) -> serde_json::Value {
    serde_json::json!({
        "schema_version": "fsqlite-e2e.hardware_discovery_bundle.v1",
        "fixture_id": record.summary.fixture_id,
        "row_id": record.row_id,
        "mode_id": record.mode_id,
        "placement_profile_id": record.placement_profile_id,
        "hardware_class_id": record.hardware_class_id,
        "hardware_signature": record.hardware_signature,
        "cpu_affinity_mask": "unspecified",
        "smt_policy_state": "host_default",
        "memory_policy": "host_default",
        "helper_lane_cpu_set": "undisclosed",
        "numa_balancing_state": "undisclosed",
        "environment": record.summary.environment,
        "required_environment_disclosures": record
            .canonical_artifact_manifest
            .provenance
            .placement_policy
            .execution_contract
            .required_environment_disclosures,
    })
}

fn hardware_discovery_summary_md(record: &CanonicalBenchmarkRecord) -> String {
    format!(
        "# Hardware Discovery\n\n- Fixture: `{}`\n- Row: `{}`\n- Mode: `{}`\n- Placement profile: `{}`\n- Hardware class: `{}`\n- Hardware signature: `{}`\n- OS: `{}`\n- Arch: `{}`\n- CPU count: `{}`\n- CPU model: `{}`\n- RAM bytes: `{}`\n- Cargo profile: `{}`\n- CPU affinity mask: `unspecified`\n- SMT policy state: `host_default`\n- Memory policy: `host_default`\n- Helper lane CPU set: `undisclosed`\n- NUMA balancing state: `undisclosed`\n",
        record.summary.fixture_id,
        record.row_id,
        record.mode_id,
        record.placement_profile_id,
        record.hardware_class_id,
        record.hardware_signature,
        record.summary.environment.os,
        record.summary.environment.arch,
        record.summary.environment.cpu_count,
        record
            .summary
            .environment
            .cpu_model
            .as_deref()
            .unwrap_or("unknown"),
        record
            .summary
            .environment
            .ram_bytes
            .map_or_else(|| "unknown".to_owned(), |bytes| bytes.to_string()),
        record.summary.environment.cargo_profile,
    )
}

fn write_canonical_bundle(
    repo_root: &Path,
    record: &CanonicalBenchmarkRecord,
) -> Result<(), Box<dyn Error>> {
    let bundle_dir = repo_root.join(&record.artifact_bundle_relpath);
    fs::create_dir_all(&bundle_dir)?;
    fs::create_dir_all(
        bundle_dir.join(&record.canonical_artifact_manifest.artifact_names.logs_dir),
    )?;
    fs::create_dir_all(
        bundle_dir.join(
            &record
                .canonical_artifact_manifest
                .artifact_names
                .profiles_dir,
        ),
    )?;
    let record_json = serde_json::to_string(record)?;
    fs::write(
        bundle_dir.join(
            &record
                .canonical_artifact_manifest
                .artifact_names
                .result_jsonl,
        ),
        format!("{record_json}\n"),
    )?;
    fs::write(
        bundle_dir.join(&record.canonical_artifact_manifest.artifact_names.summary_md),
        render_benchmark_summaries_markdown(std::slice::from_ref(&record.summary)),
    )?;
    fs::write(
        bundle_dir.join(
            &record
                .canonical_artifact_manifest
                .artifact_names
                .hardware_discovery_bundle_json,
        ),
        serde_json::to_vec_pretty(&hardware_discovery_bundle_json(record))?,
    )?;
    fs::write(
        bundle_dir.join(
            &record
                .canonical_artifact_manifest
                .artifact_names
                .hardware_discovery_summary_md,
        ),
        hardware_discovery_summary_md(record),
    )?;
    fs::write(
        bundle_dir.join(
            &record
                .canonical_artifact_manifest
                .artifact_names
                .manifest_json,
        ),
        serde_json::to_vec_pretty(&record.canonical_artifact_manifest)?,
    )?;
    Ok(())
}

struct CanonicalRewriteContext<'a> {
    command_args: &'a [String],
    repo_root: &'a Path,
    campaign: &'a fsqlite_e2e::fixture_select::BeadsBenchmarkCampaign,
    source_revision: &'a str,
    beads_data_hash: &'a str,
    run_id: &'a str,
}

fn rewrite_jsonl_with_canonical_records(
    jsonl_path: &Path,
    context: &CanonicalRewriteContext<'_>,
    generated_bundles: &mut Vec<serde_json::Value>,
) -> Result<String, Box<dyn Error>> {
    let raw = fs::read_to_string(jsonl_path)?;
    if raw.trim().is_empty() {
        return Ok(String::new());
    }
    let tool_versions = benchmark_tool_versions();
    let command_line = format_command_line(context.command_args);
    let mut enriched_lines = Vec::new();

    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let summary: BenchmarkSummary = serde_json::from_str(line)?;
        let mode = benchmark_mode_from_engine(&summary.engine)?;
        let mode_id = benchmark_mode_id(mode).to_owned();
        let cell = resolve_canonical_cell(context.campaign, &summary, mode)?;
        let manifest = build_benchmark_artifact_manifest(
            context.repo_root,
            context.campaign,
            &cell,
            BenchmarkArtifactProvenanceCapture {
                run_id: context.run_id.to_owned(),
                retention_class: BenchmarkArtifactRetentionClass::FullProof,
                command_entrypoint: "realdb-e2e bench".to_owned(),
                source_revision: context.source_revision.to_owned(),
                beads_data_hash: context.beads_data_hash.to_owned(),
                kernel_release: summary.environment.os.clone(),
                commands: vec![BenchmarkArtifactCommand {
                    tool: "realdb-e2e".to_owned(),
                    command_line: command_line.clone(),
                }],
                tool_versions: tool_versions.clone(),
                fallback_notes: Vec::new(),
            },
        )?;
        let hardware_signature = hardware_signature(context.campaign, &manifest.hardware_class_id)?;
        let artifact_manifest_path = format!(
            "{}/{}",
            manifest.artifact_bundle_relpath, manifest.artifact_names.manifest_json
        );
        let record = CanonicalBenchmarkRecord {
            summary: summary.clone(),
            row_id: manifest.row_id.clone(),
            mode_id,
            retry_policy_id: manifest.retry_policy_id.clone(),
            seed_policy_id: manifest.seed_policy_id.clone(),
            build_profile_id: manifest.build_profile_id.clone(),
            placement_profile_id: manifest.placement_profile_id.clone(),
            hardware_class_id: manifest.hardware_class_id.clone(),
            hardware_signature,
            source_revision: context.source_revision.to_owned(),
            beads_data_hash: context.beads_data_hash.to_owned(),
            run_id: manifest.run_id.clone(),
            artifact_bundle_key: manifest.artifact_bundle_key.clone(),
            artifact_bundle_name: manifest.artifact_bundle_name.clone(),
            artifact_bundle_dir: manifest.artifact_bundle_dir.clone(),
            artifact_bundle_relpath: manifest.artifact_bundle_relpath.clone(),
            artifact_manifest_path: artifact_manifest_path.clone(),
            campaign_manifest_path: manifest.campaign_manifest_path.clone(),
            canonical_artifact_manifest: manifest,
        };
        write_canonical_bundle(context.repo_root, &record)?;
        generated_bundles.push(serde_json::json!({
            "benchmark_id": record.summary.benchmark_id,
            "row_id": record.row_id,
            "mode_id": record.mode_id,
            "artifact_bundle_relpath": record.artifact_bundle_relpath,
            "artifact_manifest_path": record.artifact_manifest_path,
        }));
        enriched_lines.push(serde_json::to_string(&record)?);
    }

    let enriched = if enriched_lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", enriched_lines.join("\n"))
    };
    fs::write(jsonl_path, &enriched)?;
    Ok(enriched)
}

/// Repo-relative evidence path for one scorecard at one tested commit.
///
/// Keyed by `source_revision` so each release writes a *new* directory and
/// never mutates a previous release's evidence. That is required, not
/// cosmetic: the phase-five guard demands `tested_commit..HEAD` be an exact
/// evidence-only delta and validates artifacts by BLAKE3, so an unversioned
/// shared path would rewrite immutable evidence on the next release and would
/// also let a stale scorecard from an unrelated commit satisfy the gate.
fn release_evidence_relpath(source_revision: &str, suffix: &str) -> String {
    format!("{RELEASE_EVIDENCE_PREFIX}/{source_revision}/{suffix}")
}

/// Absolute paths to the two release scorecards the overlay honesty gate
/// requires, resolved from the validated tested commit.
///
/// Takes no environment: the release guard binds `receipt.execution.argv` but
/// does not attest a run's environment, so an env-selected scorecard path would
/// let a receipt carrying the exact canonical argv aim the gate at an arbitrary
/// file — or at nothing — undetectably.
///
/// `source_revision` must be a 40-digit lowercase hex object name, matching the
/// guard's own commit spelling rule. Rejecting anything else also rejects path
/// traversal by construction: no accepted value can contain `/`, `.` or `..`.
/// Whether the files exist is the gate's business — a missing scorecard must
/// fail the gate, never silently disable it.
fn release_scorecard_paths(
    repo_root: &Path,
    source_revision: &str,
) -> Result<(PathBuf, PathBuf), String> {
    if source_revision.len() != 40
        || !source_revision
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!(
            "release evidence requires a 40-digit lowercase hexadecimal source revision, found `{source_revision}`"
        ));
    }
    Ok((
        repo_root.join(release_evidence_relpath(
            source_revision,
            RELEASE_C1_SCORECARD_SUFFIX,
        )),
        repo_root.join(release_evidence_relpath(
            source_revision,
            RELEASE_PERSISTENT_SCORECARD_SUFFIX,
        )),
    ))
}

/// Build the overlay honesty gate configuration for this release test.
///
/// Unconditionally strict: both the c1 and persistent packs are required and
/// warnings are fatal. There is deliberately no relaxation path and no
/// environment input.
///
/// The release guard binds `receipt.execution.argv` but does **not** attest the
/// environment of a run. Any env-selected relaxation would therefore be a
/// machine-invisible bypass: a receipt carrying the exact canonical argv could
/// be produced by a run that had silently switched the pack requirements off,
/// and the guard could not tell the difference. Since this test exists solely
/// to be replayed under the canonical `run_for_release` argv, the only honest
/// configuration is the one that cannot be weakened from outside.
fn overlay_gate_config(
    matrix_thresholds: MatrixRegressionThresholds,
) -> fsqlite_e2e::overlay_honesty_gate::OverlayHonestyGateConfig {
    fsqlite_e2e::overlay_honesty_gate::OverlayHonestyGateConfig {
        matrix_thresholds,
        ..fsqlite_e2e::overlay_honesty_gate::OverlayHonestyGateConfig::strict_overlay()
    }
}

/// Keeper: the release scorecards are read from fixed canonical repo-relative
/// paths, with no environment input.
///
/// Pinning the literal strings is the point — if either constant is changed or
/// made configurable, this fails, because the guard cannot detect a run that
/// silently pointed the gate somewhere else.
#[test]
fn release_scorecard_paths_are_fixed_and_canonical() {
    let revision = "0123456789abcdef0123456789abcdef01234567";
    let root = Path::new("/repo");

    assert_eq!(
        release_evidence_relpath(revision, RELEASE_C1_SCORECARD_SUFFIX),
        format!("tests/artifacts/release-evidence/{revision}/performance/c1/c1_scorecard.json")
    );
    assert_eq!(
        release_evidence_relpath(revision, RELEASE_PERSISTENT_SCORECARD_SUFFIX),
        format!(
            "tests/artifacts/release-evidence/{revision}/performance/persistent/persistent_scorecard.json"
        )
    );

    let (c1, persistent) =
        release_scorecard_paths(root, revision).expect("40-hex revision must resolve");
    assert_eq!(
        c1,
        root.join(release_evidence_relpath(
            revision,
            RELEASE_C1_SCORECARD_SUFFIX
        ))
    );
    assert_eq!(
        persistent,
        root.join(release_evidence_relpath(
            revision,
            RELEASE_PERSISTENT_SCORECARD_SUFFIX
        ))
    );
    assert_ne!(c1, persistent, "the two scorecards must be distinct files");
    assert!(
        c1.starts_with(root) && persistent.starts_with(root),
        "scorecards must resolve inside the repository"
    );

    // Evidence is append-only: a different commit must never resolve to a path
    // that could overwrite or reuse another commit's scorecards.
    let other = "89abcdef0123456789abcdef0123456789abcdef";
    let (other_c1, _) = release_scorecard_paths(root, other).expect("second revision resolves");
    assert_ne!(
        c1, other_c1,
        "scorecard paths must be keyed by the tested commit"
    );

    // Only a 40-digit lowercase hex revision is accepted, which rejects
    // traversal and uppercase/short/empty spellings by construction.
    for bad in [
        "",
        "0123456789abcdef",
        "0123456789ABCDEF0123456789ABCDEF01234567",
        "../../../etc/passwd",
        "0123456789abcdef0123456789abcdef0123456/",
        "g123456789abcdef0123456789abcdef01234567",
    ] {
        assert!(
            release_scorecard_paths(root, bad).is_err(),
            "`{bad}` must be rejected as a source revision"
        );
    }
}

/// Keeper: the v0.2.0 historical-regression bootstrap contract.
///
/// No canonical historical complete-matrix baseline exists — not in-tree, and
/// not in the v0.1.17/v0.1.18/v0.1.19 tags, which do not contain this file at
/// all. So the release must not invent a predecessor, must not accept an
/// env-selected baseline, and must record the absence explicitly rather than
/// letting the comparison be silently skipped.
#[test]
fn matrix_regression_is_explicit_bootstrap_with_no_env_baseline() {
    assert_eq!(
        MATRIX_REGRESSION_STATUS_BOOTSTRAP, "unavailable_bootstrap_v0_2_0",
        "the bootstrap status is part of the manifest contract"
    );

    // The baseline env var is refused like every other weakening input, so a
    // canonical-argv receipt cannot smuggle in an arbitrary comparison target
    // nor silently omit the comparison.
    assert_eq!(
        forbidden_matrix_env_present(|name| name == "FSQLITE_MATRIX_BASELINE_JSONL"),
        vec!["FSQLITE_MATRIX_BASELINE_JSONL"],
        "an env-selected baseline must be refused at v0.2.0"
    );
    assert!(
        RELEASE_FORBIDDEN_MATRIX_ENV.contains(&"FSQLITE_MATRIX_BASELINE_JSONL"),
        "the baseline variable belongs in the forbidden set, not the allowed set"
    );

    // The release remains hard-gated by the commit-keyed scorecards even though
    // historical regression is unavailable.
    let config = overlay_gate_config(MatrixRegressionThresholds::default());
    assert!(
        config.require_c1_pack && config.require_persistent_pack,
        "bootstrap must not relax the scorecard gate that still applies"
    );

    // The escape expires with the version: it is valid only at 0.2.0, and the
    // crate this test ships in must actually be that version today.
    assert!(bootstrap_allowed_for_version(MATRIX_BOOTSTRAP_PKG_VERSION).is_ok());
    assert!(
        bootstrap_allowed_for_version(env!("CARGO_PKG_VERSION")).is_ok(),
        "this crate is past the bootstrap version; wire the real historical comparison"
    );
    for later in ["0.2.1", "0.3.0", "1.0.0", "0.2.0-rc.1", "0.20.0", ""] {
        assert!(
            bootstrap_allowed_for_version(later).is_err(),
            "`{later}` must not inherit the v0.2.0 bootstrap escape"
        );
    }
}

/// Negative keepers: every weakening bypass class must be refused.
///
/// Pure — the lookup is injected, so no process-global environment is mutated
/// and the test is order-independent.
#[test]
fn release_matrix_refuses_every_weakening_env_bypass() {
    assert!(
        forbidden_matrix_env_present(|_| false).is_empty(),
        "a clean environment must be accepted"
    );

    // One representative per bypass class named in the review: subsetting,
    // duration, mode, and threshold relaxation.
    for (class, name) in [
        ("subset", "FSQLITE_MATRIX_DB"),
        ("subset", "FSQLITE_MATRIX_WORKLOAD"),
        ("subset", "FSQLITE_MATRIX_CONCURRENCY"),
        ("duration", "FSQLITE_MATRIX_WARMUP"),
        ("duration", "FSQLITE_MATRIX_REPEAT"),
        ("duration", "FSQLITE_MATRIX_MIN_ITERS"),
        ("duration", "FSQLITE_MATRIX_TIME_SECS"),
        ("mode", "FSQLITE_MATRIX_MODE"),
        ("threshold", "FSQLITE_MATRIX_MAX_P95_RATIO"),
        ("threshold", "FSQLITE_MATRIX_MIN_THROUGHPUT_RATIO"),
    ] {
        assert_eq!(
            forbidden_matrix_env_present(|candidate| candidate == name),
            vec![name],
            "{class} bypass via {name} must be refused"
        );
    }

    // All at once must report all of them, not just the first.
    assert_eq!(
        forbidden_matrix_env_present(|_| true).len(),
        RELEASE_FORBIDDEN_MATRIX_ENV.len()
    );

    // Output-location variables cannot weaken a result, so they are not
    // refused; they are recorded in the manifest instead.
    for allowed in ["FSQLITE_MATRIX_OUTPUT_STEM", "FSQLITE_MATRIX_ARTIFACT_DIR"] {
        assert!(
            forbidden_matrix_env_present(|candidate| candidate == allowed).is_empty(),
            "{allowed} only relocates output and must remain permitted"
        );
    }
}

/// Keeper: the overlay honesty gate config is strict with no way to weaken it.
///
/// `overlay_gate_config` is pure and takes no environment, so this asserts
/// without mutating process-global state — mutating it would make the test
/// order-dependent, the hazard frankensqlite#299 was filed about.
#[test]
fn overlay_honesty_gate_config_is_unconditionally_strict() {
    let config = overlay_gate_config(MatrixRegressionThresholds::default());

    // Thresholds are the pinned defaults, never environment-derived.
    assert!(
        (config.matrix_thresholds.max_p95_ratio
            - fsqlite_e2e::overlay_honesty_gate::DEFAULT_MAX_P95_RATIO)
            .abs()
            < f64::EPSILON,
        "release max_p95_ratio must be the pinned default"
    );
    assert!(
        (config.matrix_thresholds.min_throughput_ratio
            - fsqlite_e2e::overlay_honesty_gate::DEFAULT_MIN_THROUGHPUT_RATIO)
            .abs()
            < f64::EPSILON,
        "release min_throughput_ratio must be the pinned default"
    );

    assert!(
        config.require_c1_pack,
        "release matrix must require the c1 pack"
    );
    assert!(
        config.require_persistent_pack,
        "release matrix must require the persistent pack"
    );
    assert!(
        config.fail_on_warning,
        "release matrix must treat gate warnings as failures"
    );
    assert_eq!(
        config,
        fsqlite_e2e::overlay_honesty_gate::OverlayHonestyGateConfig {
            matrix_thresholds: MatrixRegressionThresholds::default(),
            ..fsqlite_e2e::overlay_honesty_gate::OverlayHonestyGateConfig::strict_overlay()
        },
        "the only permitted config is strict_overlay with the supplied thresholds"
    );
}

#[test]
#[ignore = "Runs the complete canonical benchmark matrix and writes artifact bundles."]
fn complete_benchmark_matrix() -> Result<(), Box<dyn Error>> {
    // First gate, before every other check and before any workload or artifact
    // work: the bootstrap concession is valid at exactly one version. A later
    // release reaching this test at all means the historical comparison was
    // never wired, so refuse immediately rather than after spending a matrix
    // run and writing a pack that would look citable.
    bootstrap_allowed_for_version(env!("CARGO_PKG_VERSION"))?;

    // Fail closed before doing any work: the receipt cannot attest environment,
    // so a weakening variable must abort the run rather than silently produce a
    // receipt for a reduced measurement.
    let forbidden = forbidden_matrix_env_present(|name| std::env::var_os(name).is_some());
    if !forbidden.is_empty() {
        return Err(format!(
            "release matrix refuses to run with weakening environment set: {forbidden:?}; \
             the release receipt binds argv but not environment, so these cannot be trusted"
        )
        .into());
    }

    let repo_root = repo_root();
    let campaign = load_beads_benchmark_campaign(&repo_root)
        .map_err(|error| format!("load canonical Beads benchmark campaign: {error}"))?;
    let run_id = matrix_run_id();
    let source_revision = git_head_revision(&repo_root)?;
    let beads_data_hash = sha256_file(&repo_root.join(&campaign.beads_data_relpath))?;
    let artifact_dir = artifact_dir(
        &repo_root,
        &campaign,
        &run_id,
        &source_revision,
        &beads_data_hash,
    );
    fs::create_dir_all(&artifact_dir)?;

    let fixture_filter = std::env::var("FSQLITE_MATRIX_DB").ok();
    let workload_filter = std::env::var("FSQLITE_MATRIX_WORKLOAD").ok();
    let concurrency_filter = std::env::var("FSQLITE_MATRIX_CONCURRENCY").ok();
    let mode = std::env::var("FSQLITE_MATRIX_MODE").unwrap_or_else(|_| "full".to_owned());
    validate_matrix_mode(&mode)?;
    let stem = std::env::var("FSQLITE_MATRIX_OUTPUT_STEM").unwrap_or_else(|_| {
        fixture_filter
            .clone()
            .unwrap_or_else(|| "matrix".to_owned())
            .replace(',', "_")
    });

    let both_jsonl = artifact_dir.join(format!("{stem}_both.jsonl"));
    let both_md = artifact_dir.join(format!("{stem}_both.md"));
    let single_jsonl = artifact_dir.join(format!("{stem}_single_writer.jsonl"));
    let single_md = artifact_dir.join(format!("{stem}_single_writer.md"));
    let full_jsonl = artifact_dir.join(format!("{stem}_full.jsonl"));
    let manifest_json = artifact_dir.join(format!("{stem}.manifest.json"));
    let overlay_honesty_gate_json = artifact_dir.join(format!("{stem}.overlay_honesty_gate.json"));

    let mut ran_both = false;
    let mut ran_single = false;
    let mut ran_sqlite = false;
    let mut ran_mvcc = false;
    let mut both_command_args: Option<Vec<String>> = None;
    let mut single_command_args: Option<Vec<String>> = None;

    if mode == "sqlite3" {
        let mut args = vec![
            "bench".to_owned(),
            "--engine".to_owned(),
            "sqlite3".to_owned(),
            "--output-jsonl".to_owned(),
            both_jsonl.to_string_lossy().into_owned(),
            "--output-md".to_owned(),
            both_md.to_string_lossy().into_owned(),
        ];
        apply_common_filters(&mut args);
        let sqlite = run_bench("sqlite3", &args)?;
        assert_success(sqlite, "sqlite3 bench")?;
        ran_sqlite = true;
        both_command_args = Some(args);
    }

    if mode == "mvcc" {
        let mut args = vec![
            "bench".to_owned(),
            "--engine".to_owned(),
            "fsqlite".to_owned(),
            "--mvcc".to_owned(),
            "--output-jsonl".to_owned(),
            both_jsonl.to_string_lossy().into_owned(),
            "--output-md".to_owned(),
            both_md.to_string_lossy().into_owned(),
        ];
        apply_common_filters(&mut args);
        let mvcc = run_bench("mvcc", &args)?;
        assert_success(mvcc, "fsqlite mvcc bench")?;
        ran_mvcc = true;
        both_command_args = Some(args);
    }

    if mode == "full" || mode == "both" {
        let mut args = vec![
            "bench".to_owned(),
            "--output-jsonl".to_owned(),
            both_jsonl.to_string_lossy().into_owned(),
            "--output-md".to_owned(),
            both_md.to_string_lossy().into_owned(),
        ];
        apply_common_filters(&mut args);
        let both = run_bench("both", &args)?;
        assert_success(both, "canonical both-mode bench")?;
        ran_both = true;
        both_command_args = Some(args);
    }

    if mode == "full" || mode == "single_writer" {
        let mut args = vec![
            "bench".to_owned(),
            "--engine".to_owned(),
            "fsqlite".to_owned(),
            "--no-mvcc".to_owned(),
            "--output-jsonl".to_owned(),
            single_jsonl.to_string_lossy().into_owned(),
            "--output-md".to_owned(),
            single_md.to_string_lossy().into_owned(),
        ];
        apply_common_filters(&mut args);
        let single = run_bench("single_writer", &args)?;
        assert_success(single, "canonical single-writer bench")?;
        ran_single = true;
        single_command_args = Some(args);
    }

    let mut generated_bundles = Vec::new();

    let mut combined = String::new();
    if ran_both || ran_sqlite || ran_mvcc {
        let args = both_command_args
            .as_ref()
            .ok_or("missing canonical both-mode command arguments")?;
        let rewrite_context = CanonicalRewriteContext {
            command_args: args,
            repo_root: &repo_root,
            campaign: &campaign,
            source_revision: &source_revision,
            beads_data_hash: &beads_data_hash,
            run_id: &run_id,
        };
        combined.push_str(&rewrite_jsonl_with_canonical_records(
            &both_jsonl,
            &rewrite_context,
            &mut generated_bundles,
        )?);
    }
    if ran_single {
        let args = single_command_args
            .as_ref()
            .ok_or("missing canonical single-writer command arguments")?;
        let rewrite_context = CanonicalRewriteContext {
            command_args: args,
            repo_root: &repo_root,
            campaign: &campaign,
            source_revision: &source_revision,
            beads_data_hash: &beads_data_hash,
            run_id: &run_id,
        };
        combined.push_str(&rewrite_jsonl_with_canonical_records(
            &single_jsonl,
            &rewrite_context,
            &mut generated_bundles,
        )?);
    }
    fs::write(&full_jsonl, combined)?;

    // v0.2.0 bootstrap: there is no canonical historical complete-matrix
    // baseline. A repo/release audit found none in the tree and none in the
    // v0.1.17/v0.1.18/v0.1.19 tags — those tags do not even contain this file,
    // and the v0.1.17 GitHub release carries only binaries, SBOM, provenance,
    // and checksums. An env-selected baseline is therefore refused (see
    // RELEASE_FORBIDDEN_MATRIX_ENV): the receipt binds argv but not
    // environment, so a caller could otherwise point the historical comparison
    // at any file, or omit it and skip the comparison silently.
    //
    // The historical regression report is consequently recorded as explicitly
    // UNAVAILABLE/bootstrap in the manifest rather than being quietly absent,
    // and this run's full JSONL is marked as the immutable baseline for the
    // next release. The current release stays hard-gated by the commit-keyed
    // c1 + persistent scorecards, which are required unconditionally.
    // Version gate already cleared at the top of this test.
    let matrix_regression_status = MATRIX_REGRESSION_STATUS_BOOTSTRAP;
    // Release thresholds are pinned, never read from the environment: the
    // receipt binds argv but not env, so `from_env()` here would let
    // FSQLITE_MATRIX_MAX_P95_RATIO=1e300 (or a tiny min-throughput) pass the
    // gate under the exact canonical argv, undetectably.
    let matrix_thresholds = MatrixRegressionThresholds::default();

    let (overlay_c1_scorecard_json, overlay_persistent_scorecard_json) =
        release_scorecard_paths(&repo_root, &source_revision)?;
    let overlay_honesty_gate_report = {
        let report = evaluate_overlay_honesty_gate_from_paths(
            &full_jsonl,
            // No historical baseline at v0.2.0 bootstrap; see
            // MATRIX_REGRESSION_STATUS_BOOTSTRAP. Passing None explicitly
            // rather than an env-selected path is the whole point.
            None,
            Some(overlay_c1_scorecard_json.as_path()),
            Some(overlay_persistent_scorecard_json.as_path()),
            overlay_gate_config(matrix_thresholds),
        )?;
        fs::write(
            &overlay_honesty_gate_json,
            serde_json::to_vec_pretty(&report)?,
        )?;
        if let Some(summary) = report.failure_summary() {
            return Err(format!("overlay honesty gate failed: {summary}").into());
        }
        Some(report)
    };

    let manifest = serde_json::json!({
        "schema_version": "fsqlite-e2e.complete_benchmark_matrix_manifest.v2",
        "campaign_id": campaign.campaign_id,
        "campaign_manifest_path": BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE,
        "run_id": run_id,
        "source_revision": source_revision,
        "beads_data_hash": beads_data_hash,
        "artifact_dir": artifact_dir,
        "fixture_filter": fixture_filter,
        "workload_filter": workload_filter,
        "concurrency_filter": concurrency_filter,
        "mode": mode,
        "matrix_both_jsonl": (ran_both || ran_sqlite || ran_mvcc).then_some(both_jsonl),
        "matrix_both_md": (ran_both || ran_sqlite || ran_mvcc).then_some(both_md),
        "matrix_single_writer_jsonl": ran_single.then_some(single_jsonl),
        "matrix_single_writer_md": ran_single.then_some(single_md),
        "matrix_full_jsonl": full_jsonl,
        // Historical regression is explicitly unavailable at v0.2.0 bootstrap —
        // recorded as a status, never as a silently-absent field.
        "matrix_regression_status": matrix_regression_status,
        "matrix_regression_unavailable_reason":
            "no canonical historical complete-matrix baseline exists in-tree or in the \
             v0.1.17/v0.1.18/v0.1.19 tags; env-selected baselines are refused because the \
             release receipt binds argv but not environment",
        // This run's full JSONL is the immutable baseline for the next release.
        "matrix_baseline_for_next_release_jsonl": full_jsonl,
        "matrix_baseline_jsonl": serde_json::Value::Null,
        "matrix_regression_summary_json": serde_json::Value::Null,
        "matrix_regression_thresholds": matrix_thresholds,
        "overlay_honesty_gate_json": overlay_honesty_gate_report.as_ref().map(|_| overlay_honesty_gate_json.clone()),
        "overlay_honesty_gate_c1_scorecard_json": overlay_c1_scorecard_json,
        "overlay_honesty_gate_persistent_scorecard_json": overlay_persistent_scorecard_json,
        // Fixed repo-relative locations, recorded so a manifest reader can
        // confirm which scorecards the gate consumed without trusting the
        // absolute paths of the machine that produced it.
        "overlay_honesty_gate_c1_scorecard_relpath":
            release_evidence_relpath(&source_revision, RELEASE_C1_SCORECARD_SUFFIX),
        "overlay_honesty_gate_persistent_scorecard_relpath":
            release_evidence_relpath(&source_revision, RELEASE_PERSISTENT_SCORECARD_SUFFIX),
        "overlay_honesty_gate_overall_verdict": overlay_honesty_gate_report.as_ref().map(|report| report.overall_verdict),
        "overlay_honesty_gate_blocking_findings": overlay_honesty_gate_report.as_ref().map(|report| report.blocking_findings.clone()),
        "generated_bundle_count": generated_bundles.len(),
        "generated_bundles": generated_bundles,
    });
    fs::write(&manifest_json, serde_json::to_vec_pretty(&manifest)?)?;

    Ok(())
}

#[test]
#[ignore = "Runs a targeted hot-path diagnosis for a single FrankenSQLite workload cell."]
fn hot_profile_diagnosis() -> Result<(), Box<dyn Error>> {
    let repo_root = repo_root();
    let campaign = load_beads_benchmark_campaign(&repo_root)
        .map_err(|error| format!("load canonical Beads benchmark campaign: {error}"))?;
    let run_id = matrix_run_id();
    let source_revision = git_head_revision(&repo_root)?;
    let beads_data_hash = sha256_file(&repo_root.join(&campaign.beads_data_relpath))?;
    let artifact_root = artifact_dir(
        &repo_root,
        &campaign,
        &run_id,
        &source_revision,
        &beads_data_hash,
    );
    fs::create_dir_all(&artifact_root)?;

    let db = std::env::var("FSQLITE_DIAG_DB")
        .or_else(|_| std::env::var("FSQLITE_MATRIX_DB"))
        .unwrap_or_else(|_| "frankensqlite".to_owned());
    let workload = std::env::var("FSQLITE_DIAG_WORKLOAD")
        .or_else(|_| std::env::var("FSQLITE_MATRIX_WORKLOAD"))
        .unwrap_or_else(|_| "commutative_inserts_disjoint_keys".to_owned());
    let concurrency = std::env::var("FSQLITE_DIAG_CONCURRENCY")
        .or_else(|_| std::env::var("FSQLITE_MATRIX_CONCURRENCY"))
        .unwrap_or_else(|_| "8".to_owned());
    let scale = std::env::var("FSQLITE_DIAG_SCALE").unwrap_or_else(|_| "100".to_owned());
    let output_dir = std::env::var_os("FSQLITE_DIAG_OUTPUT_DIR").map_or_else(
        || artifact_root.join(format!("{db}_{workload}_c{concurrency}_hot_profile")),
        |path| {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                repo_root.join(path)
            }
        },
    );
    fs::create_dir_all(&output_dir)?;

    let pretty_json = output_dir.join("report.pretty.json");
    let mut args = vec![
        "hot-profile".to_owned(),
        "--db".to_owned(),
        db.clone(),
        "--workload".to_owned(),
        workload.clone(),
        "--concurrency".to_owned(),
        concurrency.clone(),
        "--scale".to_owned(),
        scale,
        "--output-dir".to_owned(),
        output_dir.to_string_lossy().into_owned(),
        "--pretty".to_owned(),
        "--mvcc".to_owned(),
    ];
    if std::env::var("FSQLITE_DIAG_INTEGRITY")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1")
    {
        args.push("--integrity-check".to_owned());
    }

    let (status, stdout) = run_command_capture_stdout(&args)?;
    if !stdout.trim().is_empty() {
        fs::write(&pretty_json, stdout.as_bytes())?;
        if let Ok(report) = serde_json::from_str::<serde_json::Value>(&stdout) {
            let engine_report = &report["engine_report"];
            let notes = engine_report["correctness"]["notes"].as_str().unwrap_or("");
            let error = engine_report["error"].as_str().unwrap_or("");
            println!(
                "[hot-profile] fixture={db} workload={workload} c={concurrency} ops/s={:.1} retries={} aborts={} error={} notes={}",
                engine_report["ops_per_sec"].as_f64().unwrap_or(0.0),
                engine_report["retries"].as_u64().unwrap_or(0),
                engine_report["aborts"].as_u64().unwrap_or(0),
                error,
                notes,
            );
        }
    }

    assert_success(status, "hot-profile diagnosis")?;
    Ok(())
}
