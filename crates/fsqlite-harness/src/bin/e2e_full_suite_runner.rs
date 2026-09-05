use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::e2e_orchestrator::{
    ExecutionManifest, ManifestExecutionMode, ManifestRunScope, RetryPolicy,
    build_default_manifest, build_execution_manifest, execute_manifest,
};

#[derive(Debug)]
struct CliConfig {
    execute: bool,
    scripts: Vec<String>,
    no_retry: bool,
    root_seed: Option<u64>,
    workspace_root: PathBuf,
    run_dir: PathBuf,
    summary_out: Option<PathBuf>,
    manifest_out: Option<PathBuf>,
}

fn default_workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn print_help() {
    let help = "\
e2e_full_suite_runner — canonical deterministic E2E script orchestrator (bd-mblr.4.5.2)

USAGE:
    cargo run -p fsqlite-harness --bin e2e_full_suite_runner -- [OPTIONS]

OPTIONS:
    --execute                   Execute scripts (default: dry-run summary only)
    --script <CATALOG_PATH>     Select an exact catalog path (repeatable; default: full suite)
    --no-retry                  Run each selected entry once (default: catalog retry policy)
    --root-seed <u64>           Override manifest root seed
    --workspace-root <PATH>     Workspace root (default: repo root)
    --run-dir <PATH>            Run artifact directory (default: artifacts/e2e_full_suite)
    --summary-out <PATH>        Write execution summary JSON to file
    --manifest-out <PATH>       Write manifest JSON to file
    -h, --help                  Show this help

Selected runs report their full catalog denominator and omitted paths.
Their overall_pass is always false; successful execution sets selected_scripts_pass.
";
    println!("{help}");
}

fn resolve_path(workspace_root: &Path, raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    }
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let workspace_root = default_workspace_root()?;
    let mut cfg = CliConfig {
        execute: false,
        scripts: Vec::new(),
        no_retry: false,
        root_seed: None,
        run_dir: workspace_root.join("artifacts/e2e_full_suite"),
        workspace_root,
        summary_out: None,
        manifest_out: None,
    };

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--execute" => cfg.execute = true,
            "--no-retry" => cfg.no_retry = true,
            "--script" => {
                i += 1;
                if i >= args.len() || args[i].is_empty() || args[i].starts_with('-') {
                    return Err("--script requires an exact catalog path".to_owned());
                }
                cfg.scripts.push(args[i].clone());
            }
            "--root-seed" => {
                i += 1;
                if i >= args.len() {
                    return Err("--root-seed requires a value".to_owned());
                }
                let seed = args[i]
                    .parse::<u64>()
                    .map_err(|_| format!("invalid --root-seed value: {}", args[i]))?;
                cfg.root_seed = Some(seed);
            }
            "--workspace-root" => {
                i += 1;
                if i >= args.len() {
                    return Err("--workspace-root requires a value".to_owned());
                }
                cfg.workspace_root = resolve_path(&cfg.workspace_root, &args[i]);
                if !cfg.workspace_root.exists() {
                    return Err(format!(
                        "workspace root does not exist: {}",
                        cfg.workspace_root.display()
                    ));
                }
                if cfg.run_dir == default_workspace_root()?.join("artifacts/e2e_full_suite") {
                    cfg.run_dir = cfg.workspace_root.join("artifacts/e2e_full_suite");
                }
            }
            "--run-dir" => {
                i += 1;
                if i >= args.len() {
                    return Err("--run-dir requires a value".to_owned());
                }
                cfg.run_dir = resolve_path(&cfg.workspace_root, &args[i]);
            }
            "--summary-out" => {
                i += 1;
                if i >= args.len() {
                    return Err("--summary-out requires a value".to_owned());
                }
                cfg.summary_out = Some(resolve_path(&cfg.workspace_root, &args[i]));
            }
            "--manifest-out" => {
                i += 1;
                if i >= args.len() {
                    return Err("--manifest-out requires a value".to_owned());
                }
                cfg.manifest_out = Some(resolve_path(&cfg.workspace_root, &args[i]));
            }
            "-h" | "--help" => {
                print_help();
                return Err(String::new());
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        i += 1;
    }

    Ok(cfg)
}

fn manifest_from_config(cfg: &CliConfig) -> Result<ExecutionManifest, String> {
    let mut manifest = if let Some(seed) = cfg.root_seed {
        build_execution_manifest(seed)
    } else {
        build_default_manifest()
    };
    if !cfg.scripts.is_empty() {
        manifest = manifest.select_scripts(&cfg.scripts)?;
    }
    if cfg.no_retry {
        for entry in &mut manifest.entries {
            entry.retry_policy = RetryPolicy::NoRetry;
        }
    }
    Ok(manifest)
}

fn run(args: &[String]) -> Result<bool, String> {
    let cfg = parse_args(args)?;
    let manifest = manifest_from_config(&cfg)?;

    let validation_errors = manifest.validate();
    if !validation_errors.is_empty() {
        return Err(format!(
            "manifest_validation_failed: {}",
            validation_errors.join("; ")
        ));
    }

    if let Some(path) = &cfg.manifest_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("manifest_out_parent_create_failed: {error}"))?;
        }
        let manifest_json = manifest
            .to_json()
            .map_err(|error| format!("manifest_json_serialize_failed: {error}"))?;
        fs::write(path, manifest_json).map_err(|error| {
            format!(
                "manifest_out_write_failed path={} error={error}",
                path.display()
            )
        })?;
    }

    let mode = if cfg.execute {
        ManifestExecutionMode::Execute
    } else {
        ManifestExecutionMode::DryRun
    };
    let summary = execute_manifest(&cfg.workspace_root, &cfg.run_dir, &manifest, mode)
        .map_err(|error| format!("manifest_execution_failed: {error}"))?;

    let summary_json = summary
        .to_json()
        .map_err(|error| format!("summary_json_serialize_failed: {error}"))?;

    if let Some(path) = &cfg.summary_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("summary_out_parent_create_failed: {error}"))?;
        }
        fs::write(path, summary_json).map_err(|error| {
            format!(
                "summary_out_write_failed path={} error={error}",
                path.display()
            )
        })?;
        println!(
            "INFO e2e_full_suite_summary_written path={} run_scope={} overall_pass={} selected_scripts_pass={}",
            path.display(),
            match summary.run_scope {
                ManifestRunScope::FullSuite => "full_suite",
                ManifestRunScope::SelectedScripts { .. } => "selected_scripts",
            },
            summary.overall_pass,
            summary.selected_scripts_pass
        );
    } else {
        println!("{summary_json}");
    }

    Ok(match summary.run_scope {
        ManifestRunScope::FullSuite => {
            if cfg.execute {
                summary.overall_pass
            } else {
                summary.missing_scenarios.is_empty()
            }
        }
        ManifestRunScope::SelectedScripts { .. } => !cfg.execute || summary.selected_scripts_pass,
    })
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(&args) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(error) if error.is_empty() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ERROR e2e_full_suite_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_harness::e2e_orchestrator::ManifestExecutionSummary;

    const MVCC_ORACLE: &str = "crates/fsqlite-e2e/tests/concurrent_writer_mvcc_oracle_e2e.rs";

    #[test]
    fn script_selection_keeps_catalog_command_and_records_no_retry() {
        let defaults =
            manifest_from_config(&parse_args(&[]).expect("default args")).expect("full catalog");
        assert_eq!(defaults.run_scope, ManifestRunScope::FullSuite);
        let original = defaults
            .entries
            .iter()
            .find(|entry| entry.path == MVCC_ORACLE)
            .expect("canonical MVCC oracle");
        let args = ["--script", MVCC_ORACLE, "--no-retry"].map(str::to_owned);
        let selected = manifest_from_config(&parse_args(&args).expect("selected args"))
            .expect("selected manifest");
        assert_eq!(selected.entries.len(), 1);
        assert_eq!(selected.entries[0].command, original.command);
        assert_eq!(selected.entries[0].seed, original.seed);
        assert_eq!(selected.entries[0].retry_policy, RetryPolicy::NoRetry);
        assert_ne!(original.retry_policy, RetryPolicy::NoRetry);
    }

    #[test]
    fn script_selection_rejects_missing_values_and_non_catalog_commands() {
        for args in [
            vec!["--script"],
            vec!["--script", ""],
            vec!["--script", "--execute"],
        ] {
            assert!(parse_args(&args.into_iter().map(str::to_owned).collect::<Vec<_>>()).is_err());
        }
        let args = ["--script", "echo injected"].map(str::to_owned);
        let error = manifest_from_config(&parse_args(&args).expect("selector argument"))
            .expect_err("only catalog paths are executable");
        assert!(error.contains("unknown executable catalog path"));
    }

    #[test]
    fn selected_dry_run_writes_subset_identity_without_execution_credit() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("manifest.json");
        let summary_path = temp.path().join("summary.json");
        let args = vec![
            "--script".to_owned(),
            MVCC_ORACLE.to_owned(),
            "--manifest-out".to_owned(),
            manifest_path.to_string_lossy().into_owned(),
            "--summary-out".to_owned(),
            summary_path.to_string_lossy().into_owned(),
        ];
        assert!(run(&args).expect("valid selected dry run"));
        let manifest: ExecutionManifest =
            serde_json::from_slice(&fs::read(manifest_path).expect("manifest artifact"))
                .expect("manifest JSON");
        let summary: ManifestExecutionSummary =
            serde_json::from_slice(&fs::read(summary_path).expect("summary artifact"))
                .expect("summary JSON");
        assert_eq!(summary.run_scope, manifest.run_scope);
        assert!(matches!(
            summary.run_scope,
            ManifestRunScope::SelectedScripts { .. }
        ));
        assert_eq!(summary.total_scripts, 1);
        assert_eq!(summary.scripts[0].attempts, 0);
        assert!(!summary.overall_pass);
        assert!(!summary.selected_scripts_pass);
        assert!(!summary.missing_scenarios.is_empty());
    }
}
