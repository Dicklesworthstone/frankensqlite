use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt::{self, Write as _};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use fsqlite_harness::e2e_log_schema::{self, ScenarioCriticality};
use fsqlite_harness::e2e_orchestrator::{
    ManifestExecutionMode, build_default_manifest, build_execution_manifest, execute_manifest,
};
use fsqlite_harness::e2e_traceability;

const BEAD_ID: &str = "bd-mblr.3.2.2";
const REPORT_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum GapReason {
    ScenarioMappingMissing,
    RequiredExecutionLaneMissing,
    CatalogEntryAbsentForManifestScenario,
}

impl fmt::Display for GapReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScenarioMappingMissing => write!(f, "missing_scenario_mapping"),
            Self::RequiredExecutionLaneMissing => write!(f, "missing_required_execution_lane"),
            Self::CatalogEntryAbsentForManifestScenario => {
                write!(f, "missing_catalog_entry_for_manifest_scenario")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum GapSeverity {
    Required,
    Informational,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ScenarioGap {
    scenario_id: String,
    reason: GapReason,
    severity: GapSeverity,
    criticality: Option<ScenarioCriticality>,
    description: Option<String>,
    covering_scripts: Vec<String>,
    replay_command: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScenarioCoverageDriftReport {
    schema_version: String,
    bead_id: String,
    generated_unix_ms: u128,
    root_seed: u64,
    total_catalog_scenarios: usize,
    required_catalog_scenarios: usize,
    total_manifest_scenarios: usize,
    total_manifest_missing: usize,
    required_gap_count: usize,
    informational_gap_count: usize,
    overall_pass: bool,
    gaps: Vec<ScenarioGap>,
}

impl ScenarioCoverageDriftReport {
    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[derive(Debug, Clone)]
struct ScenarioScriptIndex {
    scripts: Vec<String>,
    replay_command: Option<String>,
}

#[derive(Debug, Clone)]
struct Config {
    workspace_root: PathBuf,
    run_dir: PathBuf,
    output_json: Option<PathBuf>,
    output_human: Option<PathBuf>,
    root_seed: Option<u64>,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
        let mut run_dir = workspace_root.join("artifacts/scenario-coverage-drift-gate");
        let mut output_json = None;
        let mut output_human = None;
        let mut root_seed = None;

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
                }
                "--run-dir" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --run-dir".to_owned())?;
                    run_dir = PathBuf::from(value);
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
                    root_seed = Some(value.parse::<u64>().map_err(|error| {
                        format!("invalid_root_seed value={value} error={error}")
                    })?);
                }
                "--help" | "-h" => {
                    println!(
                        "\
scenario_coverage_drift_gate_runner — CI drift gate for bd-mblr.3.2.2

USAGE:
  cargo run -p fsqlite-harness --bin scenario_coverage_drift_gate_runner -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>   Workspace root (default: auto-detected)
  --run-dir <PATH>          Gate run directory (default: artifacts/scenario-coverage-drift-gate)
  --output-json <PATH>      Write machine-readable gate report JSON
  --output-human <PATH>     Write concise human summary markdown
  --root-seed <U64>         Override manifest root seed (default: canonical orchestrator seed)
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
            workspace_root,
            run_dir,
            output_json,
            output_human,
            root_seed,
        })
    }
}

fn build_scenario_index() -> BTreeMap<String, ScenarioScriptIndex> {
    let matrix = e2e_traceability::build_canonical_inventory();
    let mut index: BTreeMap<String, ScenarioScriptIndex> = BTreeMap::new();

    for script in matrix.scripts {
        for scenario_id in script.scenario_ids {
            let entry = index
                .entry(scenario_id)
                .or_insert_with(|| ScenarioScriptIndex {
                    scripts: Vec::new(),
                    replay_command: None,
                });
            if !entry.scripts.iter().any(|path| path == &script.path) {
                entry.scripts.push(script.path.clone());
                entry.scripts.sort();
            }
            if entry.replay_command.is_none() {
                entry.replay_command = Some(script.invocation.command.clone());
            }
        }
    }

    index
}

#[allow(clippy::too_many_lines)]
fn build_report(config: &Config) -> Result<ScenarioCoverageDriftReport, String> {
    fs::create_dir_all(&config.run_dir).map_err(|error| {
        format!(
            "run_dir_create_failed path={} error={error}",
            config.run_dir.display()
        )
    })?;

    let manifest = if let Some(root_seed) = config.root_seed {
        build_execution_manifest(root_seed)
    } else {
        build_default_manifest()
    };

    let manifest_summary = execute_manifest(
        &config.workspace_root,
        &config.run_dir,
        &manifest,
        ManifestExecutionMode::DryRun,
    )
    .map_err(|error| format!("execute_manifest_dry_run_failed: {error}"))?;

    let coverage_report = e2e_log_schema::build_coverage_report();
    let scenario_index = build_scenario_index();

    let required_catalog_scenarios: BTreeMap<String, (ScenarioCriticality, String)> =
        coverage_report
            .scenarios
            .iter()
            .filter(|scenario| {
                matches!(
                    scenario.criticality,
                    ScenarioCriticality::Critical | ScenarioCriticality::Important
                )
            })
            .map(|scenario| {
                (
                    scenario.scenario_id.clone(),
                    (scenario.criticality, scenario.description.clone()),
                )
            })
            .collect();

    let missing_manifest: BTreeSet<String> =
        manifest_summary.missing_scenarios.iter().cloned().collect();

    let mut gaps = Vec::new();

    for scenario in &coverage_report.scenarios {
        let is_required = matches!(
            scenario.criticality,
            ScenarioCriticality::Critical | ScenarioCriticality::Important
        );
        if !is_required {
            continue;
        }

        if !scenario.covered {
            gaps.push(ScenarioGap {
                scenario_id: scenario.scenario_id.clone(),
                reason: GapReason::ScenarioMappingMissing,
                severity: GapSeverity::Required,
                criticality: Some(scenario.criticality),
                description: Some(scenario.description.clone()),
                covering_scripts: scenario.covering_scripts.clone(),
                replay_command: scenario.replay_command.clone(),
            });
            continue;
        }

        if missing_manifest.contains(&scenario.scenario_id) {
            gaps.push(ScenarioGap {
                scenario_id: scenario.scenario_id.clone(),
                reason: GapReason::RequiredExecutionLaneMissing,
                severity: GapSeverity::Required,
                criticality: Some(scenario.criticality),
                description: Some(scenario.description.clone()),
                covering_scripts: scenario.covering_scripts.clone(),
                replay_command: scenario.replay_command.clone(),
            });
        }
    }

    for missing in &manifest_summary.missing_scenarios {
        if required_catalog_scenarios.contains_key(missing) {
            continue;
        }

        let idx = scenario_index.get(missing);
        gaps.push(ScenarioGap {
            scenario_id: missing.clone(),
            reason: GapReason::CatalogEntryAbsentForManifestScenario,
            severity: GapSeverity::Required,
            criticality: None,
            description: None,
            covering_scripts: idx.map_or_else(Vec::new, |entry| entry.scripts.clone()),
            replay_command: idx.and_then(|entry| entry.replay_command.clone()),
        });
    }

    gaps.sort_by(|a, b| {
        a.scenario_id
            .cmp(&b.scenario_id)
            .then_with(|| a.reason.cmp(&b.reason))
    });

    let required_gap_count = gaps
        .iter()
        .filter(|gap| gap.severity == GapSeverity::Required)
        .count();
    let informational_gap_count = gaps.len().saturating_sub(required_gap_count);

    Ok(ScenarioCoverageDriftReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        generated_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
        root_seed: manifest.root_seed,
        total_catalog_scenarios: coverage_report.stats.total_scenarios,
        required_catalog_scenarios: required_catalog_scenarios.len(),
        total_manifest_scenarios: manifest.coverage.total_scenario_ids,
        total_manifest_missing: manifest_summary.missing_scenarios.len(),
        required_gap_count,
        informational_gap_count,
        overall_pass: required_gap_count == 0,
        gaps,
    })
}

fn render_human_summary(report: &ScenarioCoverageDriftReport) -> String {
    let mut out = String::new();
    out.push_str("# Scenario Coverage Drift Gate\n\n");
    let _ = writeln!(out, "- bead_id: `{}`", report.bead_id);
    let _ = writeln!(out, "- schema_version: `{}`", report.schema_version);
    let _ = writeln!(out, "- root_seed: `{}`", report.root_seed);
    let _ = writeln!(
        out,
        "- total_catalog_scenarios: `{}`",
        report.total_catalog_scenarios
    );
    let _ = writeln!(
        out,
        "- required_catalog_scenarios: `{}`",
        report.required_catalog_scenarios
    );
    let _ = writeln!(
        out,
        "- total_manifest_scenarios: `{}`",
        report.total_manifest_scenarios
    );
    let _ = writeln!(
        out,
        "- total_manifest_missing: `{}`",
        report.total_manifest_missing
    );
    let _ = writeln!(out, "- required_gap_count: `{}`", report.required_gap_count);
    let _ = writeln!(
        out,
        "- informational_gap_count: `{}`",
        report.informational_gap_count
    );
    let _ = writeln!(out, "- overall_pass: `{}`", report.overall_pass);

    if report.gaps.is_empty() {
        out.push_str(
            "\nAll required scenarios are mapped and present in required execution lanes.\n",
        );
        return out;
    }

    out.push_str("\n## Gap Diff\n");
    for gap in &report.gaps {
        let _ = write!(
            out,
            "- [{}] `{}` reason=`{}` severity=`{:?}`",
            gap.criticality
                .map_or_else(|| "uncatalogued".to_owned(), |c| format!("{c:?}")),
            gap.scenario_id,
            gap.reason,
            gap.severity
        );
        if let Some(description) = &gap.description {
            let _ = write!(out, " desc=\"{description}\"");
        }
        if !gap.covering_scripts.is_empty() {
            let _ = write!(out, " scripts={:?}", gap.covering_scripts);
        }
        out.push('\n');
    }

    out
}

fn run() -> Result<bool, String> {
    let config = Config::parse()?;
    let report = build_report(&config)?;

    if let Some(path) = &config.output_json {
        let json = report
            .to_json()
            .map_err(|error| format!("report_serialize_failed: {error}"))?;
        fs::write(path, json).map_err(|error| {
            format!("report_write_failed path={} error={error}", path.display())
        })?;
        println!(
            "INFO scenario_coverage_drift_report_written path={} overall_pass={}",
            path.display(),
            report.overall_pass
        );
    } else {
        let json = report
            .to_json()
            .map_err(|error| format!("report_serialize_failed: {error}"))?;
        println!("{json}");
    }

    let summary = render_human_summary(&report);
    if let Some(path) = &config.output_human {
        fs::write(path, summary).map_err(|error| {
            format!(
                "human_summary_write_failed path={} error={error}",
                path.display()
            )
        })?;
    } else {
        eprintln!("{summary}");
    }

    Ok(report.overall_pass)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("ERROR scenario_coverage_drift_gate_runner overall_pass=false");
            ExitCode::from(1)
        }
        Err(error) => {
            eprintln!("ERROR scenario_coverage_drift_gate_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
