use std::collections::BTreeMap;
use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use fsqlite_harness::no_mock_evidence::{self, NoMockEvidenceEntry};
use fsqlite_harness::parity_taxonomy::FeatureCategory;
use fsqlite_harness::unit_matrix;

const BEAD_ID: &str = "bd-mblr.3.1.2";
const REPORT_SCHEMA_VERSION: &str = "1.0.0";
const SHARD_SUMMARY_SCHEMA_VERSION: &str = "fsqlite-ci.unit-shard-summary.v1";
const DEFAULT_REQUIRED_LANES: &[&str] = &["storage-foundation", "sql-engine"];

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum GapReason {
    MatrixValidationError,
    EvidenceValidationError,
    MissingInvariantEvidence,
    MockOnlyCriticalPathInvariant,
    MissingRequiredUnitLaneSummary,
    RequiredUnitLaneFailed,
}

impl fmt::Display for GapReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MatrixValidationError => write!(f, "matrix_validation_error"),
            Self::EvidenceValidationError => write!(f, "evidence_validation_error"),
            Self::MissingInvariantEvidence => write!(f, "missing_invariant_evidence"),
            Self::MockOnlyCriticalPathInvariant => write!(f, "mock_only_critical_path_invariant"),
            Self::MissingRequiredUnitLaneSummary => write!(f, "missing_required_unit_lane_summary"),
            Self::RequiredUnitLaneFailed => write!(f, "required_unit_lane_failed"),
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
struct UnitCoverageGap {
    reason: GapReason,
    severity: GapSeverity,
    matrix_test_id: Option<String>,
    category: Option<String>,
    invariant: Option<String>,
    lane_id: Option<String>,
    details: String,
    remediation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UnitCoverageDriftReport {
    schema_version: String,
    bead_id: String,
    generated_unix_ms: u128,
    unit_matrix_schema_version: String,
    evidence_schema_version: String,
    total_matrix_tests: usize,
    total_matrix_invariants: usize,
    unit_matrix_overall_fill_pct: f64,
    total_evidence_entries: usize,
    evidence_coverage_pct: f64,
    critical_invariant_count: usize,
    critical_invariants_with_real_evidence: usize,
    required_lane_count: usize,
    discovered_lane_count: usize,
    required_gap_count: usize,
    informational_gap_count: usize,
    overall_pass: bool,
    gaps: Vec<UnitCoverageGap>,
}

impl UnitCoverageDriftReport {
    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct UnitShardSummary {
    schema_version: String,
    shard_id: String,
    failed_crates: Vec<String>,
    failed_count: usize,
}

#[derive(Debug, Clone)]
struct Config {
    run_dir: PathBuf,
    unit_shard_root: PathBuf,
    required_lanes: Vec<String>,
    output_json: Option<PathBuf>,
    output_human: Option<PathBuf>,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))?;
        let mut run_dir = workspace_root.join("artifacts/unit-coverage-drift-gate");
        let mut unit_shard_root = workspace_root.join("artifacts/unit-shards");
        let mut required_lanes: Vec<String> = DEFAULT_REQUIRED_LANES
            .iter()
            .map(|lane| (*lane).to_owned())
            .collect();
        let mut output_json = None;
        let mut output_human = None;

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
                "--unit-shard-root" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --unit-shard-root".to_owned())?;
                    unit_shard_root = PathBuf::from(value);
                }
                "--required-lanes" => {
                    idx += 1;
                    let value = args
                        .get(idx)
                        .ok_or_else(|| "missing value for --required-lanes".to_owned())?;
                    required_lanes = parse_required_lanes(value);
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
                "--help" | "-h" => {
                    println!(
                        "\
unit_coverage_drift_gate_runner — CI drift gate for bd-mblr.3.1.2

USAGE:
  cargo run -p fsqlite-harness --bin unit_coverage_drift_gate_runner -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>     Workspace root (default: auto-detected)
  --run-dir <PATH>            Gate run directory (default: artifacts/unit-coverage-drift-gate)
  --unit-shard-root <PATH>    Directory containing unit shard summary artifacts
                              (default: artifacts/unit-shards)
  --required-lanes <CSV>      Required shard IDs (default: storage-foundation,sql-engine)
  --output-json <PATH>        Write machine-readable gate report JSON
  --output-human <PATH>       Write concise human summary markdown
  -h, --help                  Show help
"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown_argument: {other}")),
            }
            idx += 1;
        }

        if required_lanes.is_empty() {
            return Err("required_lanes_empty".to_owned());
        }

        Ok(Self {
            run_dir,
            unit_shard_root,
            required_lanes,
            output_json,
            output_human,
        })
    }
}

fn parse_required_lanes(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|lane| !lane.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn is_critical_category(category: FeatureCategory) -> bool {
    matches!(
        category,
        FeatureCategory::SqlGrammar
            | FeatureCategory::VdbeOpcodes
            | FeatureCategory::StorageTransaction
    )
}

fn index_evidence<'a>(
    entries: &'a [NoMockEvidenceEntry],
) -> BTreeMap<(String, String), Vec<&'a NoMockEvidenceEntry>> {
    let mut index: BTreeMap<(String, String), Vec<&NoMockEvidenceEntry>> = BTreeMap::new();
    for entry in entries {
        index
            .entry((entry.matrix_test_id.clone(), entry.invariant.clone()))
            .or_default()
            .push(entry);
    }
    index
}

fn collect_json_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut pending = vec![root.to_path_buf()];
    let mut json_files = Vec::new();

    while let Some(dir) = pending.pop() {
        let mut children = Vec::new();
        let read_dir = fs::read_dir(&dir).map_err(|error| {
            format!(
                "shard_root_read_dir_failed path={} error={error}",
                dir.display()
            )
        })?;
        for entry in read_dir {
            let entry = entry.map_err(|error| {
                format!(
                    "shard_root_entry_read_failed path={} error={error}",
                    dir.display()
                )
            })?;
            children.push(entry.path());
        }
        children.sort();

        for child in children {
            if child.is_dir() {
                pending.push(child);
                continue;
            }
            if child
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "json")
            {
                json_files.push(child);
            }
        }
    }

    json_files.sort();
    Ok(json_files)
}

fn discover_unit_shard_summaries(
    root: &Path,
) -> Result<BTreeMap<String, UnitShardSummary>, String> {
    let json_files = collect_json_files(root)?;
    let mut summaries: BTreeMap<String, UnitShardSummary> = BTreeMap::new();

    for file in json_files {
        let content = match fs::read_to_string(&file) {
            Ok(content) => content,
            Err(_) => continue,
        };
        let parsed = match serde_json::from_str::<UnitShardSummary>(&content) {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };
        if parsed.schema_version != SHARD_SUMMARY_SCHEMA_VERSION {
            continue;
        }
        summaries.entry(parsed.shard_id.clone()).or_insert(parsed);
    }

    Ok(summaries)
}

fn build_report(config: &Config) -> Result<UnitCoverageDriftReport, String> {
    fs::create_dir_all(&config.run_dir).map_err(|error| {
        format!(
            "run_dir_create_failed path={} error={error}",
            config.run_dir.display()
        )
    })?;

    let matrix = unit_matrix::build_canonical_matrix();
    let evidence_map = no_mock_evidence::build_evidence_map();
    let evidence_index = index_evidence(&evidence_map.entries);
    let unit_shard_summaries = discover_unit_shard_summaries(&config.unit_shard_root)?;

    let total_matrix_invariants: usize =
        matrix.tests.iter().map(|test| test.invariants.len()).sum();

    let mut gaps = Vec::new();

    let matrix_errors = matrix.validate();
    for error in matrix_errors {
        gaps.push(UnitCoverageGap {
            reason: GapReason::MatrixValidationError,
            severity: GapSeverity::Required,
            matrix_test_id: None,
            category: None,
            invariant: None,
            lane_id: None,
            details: error,
            remediation: "Fix canonical unit matrix structure/invariants before merging".to_owned(),
        });
    }

    let evidence_errors = evidence_map.validate();
    for error in evidence_errors {
        gaps.push(UnitCoverageGap {
            reason: GapReason::EvidenceValidationError,
            severity: GapSeverity::Required,
            matrix_test_id: None,
            category: None,
            invariant: None,
            lane_id: None,
            details: error,
            remediation: "Update no-mock evidence map to satisfy invariant coverage contracts"
                .to_owned(),
        });
    }

    let mut critical_invariant_count = 0_usize;
    let mut critical_invariants_with_real_evidence = 0_usize;

    for test in &matrix.tests {
        for invariant in &test.invariants {
            let key = (test.test_id.clone(), invariant.clone());
            let evidence_entries = evidence_index.get(&key).cloned().unwrap_or_default();

            if evidence_entries.is_empty() {
                gaps.push(UnitCoverageGap {
                    reason: GapReason::MissingInvariantEvidence,
                    severity: GapSeverity::Required,
                    matrix_test_id: Some(test.test_id.clone()),
                    category: Some(test.category.display_name().to_owned()),
                    invariant: Some(invariant.clone()),
                    lane_id: None,
                    details: "Invariant is present in the canonical unit matrix but has no evidence entry".to_owned(),
                    remediation:
                        "Add a no-mock evidence entry that links this invariant to a concrete real-component unit test"
                            .to_owned(),
                });
                if is_critical_category(test.category) {
                    critical_invariant_count += 1;
                }
                continue;
            }

            let has_real_non_exception = evidence_entries
                .iter()
                .any(|entry| !entry.is_exception && !entry.real_components.is_empty());

            if is_critical_category(test.category) {
                critical_invariant_count += 1;
                if has_real_non_exception {
                    critical_invariants_with_real_evidence += 1;
                } else {
                    gaps.push(UnitCoverageGap {
                        reason: GapReason::MockOnlyCriticalPathInvariant,
                        severity: GapSeverity::Required,
                        matrix_test_id: Some(test.test_id.clone()),
                        category: Some(test.category.display_name().to_owned()),
                        invariant: Some(invariant.clone()),
                        lane_id: None,
                        details:
                            "Critical-path invariant lacks non-exception real-component evidence (mock-only or exception-only)"
                                .to_owned(),
                        remediation:
                            "Add or restore a real-component unit test evidence entry for this critical-path invariant"
                                .to_owned(),
                    });
                }
            }
        }
    }

    for lane in &config.required_lanes {
        match unit_shard_summaries.get(lane) {
            Some(summary) => {
                let has_failures = summary.failed_count > 0 || !summary.failed_crates.is_empty();
                if has_failures {
                    gaps.push(UnitCoverageGap {
                        reason: GapReason::RequiredUnitLaneFailed,
                        severity: GapSeverity::Required,
                        matrix_test_id: None,
                        category: None,
                        invariant: None,
                        lane_id: Some(lane.clone()),
                        details: format!(
                            "Required unit lane failed_count={} failed_crates={:?}",
                            summary.failed_count, summary.failed_crates
                        ),
                        remediation:
                            "Fix failing crates in this required lane and regenerate shard summary artifacts".to_owned(),
                    });
                }
            }
            None => {
                gaps.push(UnitCoverageGap {
                    reason: GapReason::MissingRequiredUnitLaneSummary,
                    severity: GapSeverity::Required,
                    matrix_test_id: None,
                    category: None,
                    invariant: None,
                    lane_id: Some(lane.clone()),
                    details: format!(
                        "No shard summary found for required lane under {}",
                        config.unit_shard_root.display()
                    ),
                    remediation:
                        "Ensure unit-test shard matrix uploads summary.json artifacts for all required lanes".to_owned(),
                });
            }
        }
    }

    gaps.sort_by(|left, right| {
        left.reason
            .cmp(&right.reason)
            .then_with(|| left.matrix_test_id.cmp(&right.matrix_test_id))
            .then_with(|| left.invariant.cmp(&right.invariant))
            .then_with(|| left.lane_id.cmp(&right.lane_id))
            .then_with(|| left.details.cmp(&right.details))
    });

    let required_gap_count = gaps
        .iter()
        .filter(|gap| gap.severity == GapSeverity::Required)
        .count();
    let informational_gap_count = gaps.len().saturating_sub(required_gap_count);

    Ok(UnitCoverageDriftReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        generated_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis()),
        unit_matrix_schema_version: matrix.schema_version.clone(),
        evidence_schema_version: evidence_map.schema_version,
        total_matrix_tests: matrix.tests.len(),
        total_matrix_invariants,
        unit_matrix_overall_fill_pct: matrix.overall_fill_pct(),
        total_evidence_entries: evidence_map.entries.len(),
        evidence_coverage_pct: evidence_map.stats.coverage_pct,
        critical_invariant_count,
        critical_invariants_with_real_evidence,
        required_lane_count: config.required_lanes.len(),
        discovered_lane_count: unit_shard_summaries.len(),
        required_gap_count,
        informational_gap_count,
        overall_pass: required_gap_count == 0,
        gaps,
    })
}

fn render_human_summary(report: &UnitCoverageDriftReport) -> String {
    let mut out = String::new();
    out.push_str("# Unit Coverage Drift Gate\n\n");
    out.push_str(&format!("- bead_id: `{}`\n", report.bead_id));
    out.push_str(&format!("- schema_version: `{}`\n", report.schema_version));
    out.push_str(&format!(
        "- generated_unix_ms: `{}`\n",
        report.generated_unix_ms
    ));
    out.push_str(&format!(
        "- total_matrix_tests: `{}`\n",
        report.total_matrix_tests
    ));
    out.push_str(&format!(
        "- total_matrix_invariants: `{}`\n",
        report.total_matrix_invariants
    ));
    out.push_str(&format!(
        "- total_evidence_entries: `{}`\n",
        report.total_evidence_entries
    ));
    out.push_str(&format!(
        "- unit_matrix_overall_fill_pct: `{:.4}`\n",
        report.unit_matrix_overall_fill_pct
    ));
    out.push_str(&format!(
        "- evidence_coverage_pct: `{:.4}`\n",
        report.evidence_coverage_pct
    ));
    out.push_str(&format!(
        "- critical_invariants_with_real_evidence: `{}/{}`\n",
        report.critical_invariants_with_real_evidence, report.critical_invariant_count
    ));
    out.push_str(&format!(
        "- required_lane_count: `{}`\n",
        report.required_lane_count
    ));
    out.push_str(&format!(
        "- discovered_lane_count: `{}`\n",
        report.discovered_lane_count
    ));
    out.push_str(&format!(
        "- required_gap_count: `{}`\n",
        report.required_gap_count
    ));
    out.push_str(&format!(
        "- informational_gap_count: `{}`\n",
        report.informational_gap_count
    ));
    out.push_str(&format!("- overall_pass: `{}`\n", report.overall_pass));

    if report.gaps.is_empty() {
        out.push_str(
            "\nNo drift detected: matrix, evidence map, and required unit lanes satisfy gate criteria.\n",
        );
        return out;
    }

    out.push_str("\n## Gap Diff\n");
    for gap in &report.gaps {
        out.push_str(&format!(
            "- reason=`{}` severity=`{:?}`",
            gap.reason, gap.severity
        ));
        if let Some(test_id) = &gap.matrix_test_id {
            out.push_str(&format!(" test_id=`{test_id}`"));
        }
        if let Some(category) = &gap.category {
            out.push_str(&format!(" category=`{category}`"));
        }
        if let Some(invariant) = &gap.invariant {
            out.push_str(&format!(" invariant=\"{invariant}\""));
        }
        if let Some(lane_id) = &gap.lane_id {
            out.push_str(&format!(" lane_id=`{lane_id}`"));
        }
        out.push_str(&format!(" details=\"{}\"", gap.details));
        out.push('\n');
        out.push_str(&format!("  remediation: {}\n", gap.remediation));
    }

    out
}

fn write_text(path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "output_parent_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    fs::write(path, content)
        .map_err(|error| format!("output_write_failed path={} error={error}", path.display()))
}

fn run() -> Result<bool, String> {
    let config = Config::parse()?;
    let report = build_report(&config)?;

    if let Some(path) = &config.output_json {
        let json = report
            .to_json()
            .map_err(|error| format!("report_serialize_failed: {error}"))?;
        write_text(path, &json)?;
        println!(
            "INFO unit_coverage_drift_report_written path={} overall_pass={}",
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
        write_text(path, &summary)?;
    } else {
        eprintln!("{summary}");
    }

    Ok(report.overall_pass)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("ERROR unit_coverage_drift_gate_runner overall_pass=false");
            ExitCode::from(1)
        }
        Err(error) => {
            eprintln!("ERROR unit_coverage_drift_gate_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
