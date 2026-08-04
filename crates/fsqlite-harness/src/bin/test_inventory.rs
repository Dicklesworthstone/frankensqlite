use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_harness::test_inventory::{
    AdoptionDecision, DEFAULT_TURSO_INVENTORY_CONTRACT_PATH, InventoryRunMetadata,
    build_test_inventory_report, write_test_inventory_outputs,
};

#[derive(Debug, Clone)]
struct Config {
    workspace_root: PathBuf,
    contract_path: PathBuf,
    upstream_tree_path: Option<PathBuf>,
    require_upstream_tree: bool,
    output_json: PathBuf,
    output_markdown: PathBuf,
    output_csv: PathBuf,
    run_id: String,
    trace_id: String,
    command: String,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut workspace_root = default_workspace_root()?;
        let mut contract_path = PathBuf::from(DEFAULT_TURSO_INVENTORY_CONTRACT_PATH);
        let mut upstream_tree_path = None;
        let mut require_upstream_tree = false;
        let mut output_json = PathBuf::from("target/test-inventory/test_inventory.json");
        let mut output_markdown = PathBuf::from("target/test-inventory/summary.md");
        let mut output_csv = PathBuf::from("target/test-inventory/test_inventory.csv");
        let generated_unix_ms = current_unix_ms()?;
        let mut run_id = format!("turso-test-inventory-{generated_unix_ms}");
        let mut trace_id = format!("trace-{run_id}");
        let args = env::args().collect::<Vec<_>>();
        let command = args.join(" ");
        let mut index = 1_usize;

        while index < args.len() {
            match args[index].as_str() {
                "--workspace-root" => {
                    index += 1;
                    workspace_root = required_path(&args, index, "--workspace-root")?;
                }
                "--contract" => {
                    index += 1;
                    contract_path = required_path(&args, index, "--contract")?;
                }
                "--upstream-tree" => {
                    index += 1;
                    upstream_tree_path = Some(required_path(&args, index, "--upstream-tree")?);
                }
                "--require-upstream-tree" => require_upstream_tree = true,
                "--output-json" => {
                    index += 1;
                    output_json = required_path(&args, index, "--output-json")?;
                }
                "--output-markdown" => {
                    index += 1;
                    output_markdown = required_path(&args, index, "--output-markdown")?;
                }
                "--output-csv" => {
                    index += 1;
                    output_csv = required_path(&args, index, "--output-csv")?;
                }
                "--run-id" => {
                    index += 1;
                    run_id = required_string(&args, index, "--run-id")?;
                }
                "--trace-id" => {
                    index += 1;
                    trace_id = required_string(&args, index, "--trace-id")?;
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown_argument: {other}")),
            }
            index += 1;
        }

        if require_upstream_tree && upstream_tree_path.is_none() {
            return Err(
                "upstream_tree_required: pass --upstream-tree <GitHub tree JSON>".to_owned(),
            );
        }

        Ok(Self {
            workspace_root,
            contract_path,
            upstream_tree_path,
            require_upstream_tree,
            output_json,
            output_markdown,
            output_csv,
            run_id,
            trace_id,
            command,
        })
    }
}

fn required_path(args: &[String], index: usize, flag: &str) -> Result<PathBuf, String> {
    required_string(args, index, flag).map(PathBuf::from)
}

fn required_string(args: &[String], index: usize, flag: &str) -> Result<String, String> {
    args.get(index)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn current_unix_ms() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|error| format!("system_time_before_unix_epoch: {error}"))
}

fn default_workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn resolve_workspace_path(workspace_root: &Path, path: &Path) -> PathBuf {
    if path.is_relative() {
        workspace_root.join(path)
    } else {
        path.to_path_buf()
    }
}

fn print_help() {
    println!(
        "\
test_inventory - tracked-source test realism and Turso adaptation audit

USAGE:
  cargo run -p fsqlite-harness --bin test_inventory -- [OPTIONS]

OPTIONS:
  --workspace-root <PATH>    Workspace root (default: auto-detected repository root)
  --contract <PATH>          Intake contract (default: docs/contracts/turso_test_adaptation_inventory.toml)
  --upstream-tree <PATH>     Pinned GitHub recursive tree JSON to verify
  --require-upstream-tree    Fail unless --upstream-tree is provided
  --output-json <PATH>       Machine report (default: target/test-inventory/test_inventory.json)
  --output-markdown <PATH>   Human report (default: target/test-inventory/summary.md)
  --output-csv <PATH>        Per-file CSV (default: target/test-inventory/test_inventory.csv)
  --run-id <ID>              Stable run identifier
  --trace-id <ID>            Stable trace identifier
  -h, --help                 Show this help
"
    );
}

fn run() -> Result<ExitCode, String> {
    let config = Config::parse()?;
    let contract_path = resolve_workspace_path(&config.workspace_root, &config.contract_path);
    let upstream_tree_path = config
        .upstream_tree_path
        .as_deref()
        .map(|path| resolve_workspace_path(&config.workspace_root, path));
    if config.require_upstream_tree && upstream_tree_path.is_none() {
        return Err("upstream_tree_required".to_owned());
    }
    let output_json = resolve_workspace_path(&config.workspace_root, &config.output_json);
    let output_markdown = resolve_workspace_path(&config.workspace_root, &config.output_markdown);
    let output_csv = resolve_workspace_path(&config.workspace_root, &config.output_csv);
    let run_metadata = InventoryRunMetadata::now(config.run_id, config.trace_id, config.command)?;
    let report = build_test_inventory_report(
        &config.workspace_root,
        &contract_path,
        upstream_tree_path.as_deref(),
        run_metadata,
    )?;
    write_test_inventory_outputs(&report, &output_json, &output_markdown, &output_csv)?;

    let adopted = report
        .decision_totals
        .get(&AdoptionDecision::Adopt)
        .copied()
        .unwrap_or_default();
    let deferred = report
        .decision_totals
        .get(&AdoptionDecision::Defer)
        .copied()
        .unwrap_or_default();
    let rejected = report
        .decision_totals
        .get(&AdoptionDecision::Reject)
        .copied()
        .unwrap_or_default();
    let test_class_files = report
        .summary
        .classes
        .iter()
        .map(|summary| (summary.class, summary.file_count))
        .collect::<BTreeMap<_, _>>();
    let test_class_files = serde_json::to_string(&test_class_files)
        .map_err(|error| format!("test_class_summary_serialize_failed: {error}"))?;
    let duplicate_owner_refs =
        report
            .portfolio
            .iter()
            .fold(BTreeMap::<&str, usize>::new(), |mut totals, entry| {
                for owner in &entry.duplicate_owners {
                    *totals.entry(owner).or_default() += 1;
                }
                totals
            });
    let duplicate_owner_refs = serde_json::to_string(&duplicate_owner_refs)
        .map_err(|error| format!("duplicate_owner_summary_serialize_failed: {error}"))?;
    tracing::info!(
        run_id = %report.run.run_id,
        trace_id = %report.run.trace_id,
        scenario_id = %report.run.scenario_id,
        source_revision = %report.provenance.source_revision,
        upstream_tree_verified = report.provenance.upstream_tree_verified,
        tracked_files = report.summary.tracked_test_and_corpus_files,
        direct_tests = report.summary.direct_test_attributes,
        adopted,
        deferred,
        rejected,
        test_class_files = %test_class_files,
        duplicate_groups = report.summary.duplicate_groups,
        duplicate_owner_refs = %duplicate_owner_refs,
        "test inventory completed"
    );
    println!(
        "INFO test_inventory_complete run_id={} trace_id={} scenario_id={} source_revision={} dirty={} upstream_verified={} files={} tests={} adopt={} defer={} reject={} test_class_files={} duplicate_groups={} duplicate_owner_refs={} json={} markdown={} csv={}",
        report.run.run_id,
        report.run.trace_id,
        report.run.scenario_id,
        report.provenance.source_revision,
        report.provenance.source_dirty,
        report.provenance.upstream_tree_verified,
        report.summary.tracked_test_and_corpus_files,
        report.summary.direct_test_attributes,
        adopted,
        deferred,
        rejected,
        test_class_files,
        report.summary.duplicate_groups,
        duplicate_owner_refs,
        output_json.display(),
        output_markdown.display(),
        output_csv.display()
    );
    Ok(ExitCode::SUCCESS)
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ERROR test_inventory_failed {error}");
            ExitCode::FAILURE
        }
    }
}
