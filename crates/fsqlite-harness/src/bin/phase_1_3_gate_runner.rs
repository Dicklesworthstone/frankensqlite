use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::verification_gates::{run_phase_1_to_3_gates, write_phase_gate_report};

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn run() -> Result<bool, String> {
    let root = workspace_root()?;
    let report = run_phase_1_to_3_gates(&root);

    if let Some(output_path) = env::args_os().nth(1) {
        let output_path = PathBuf::from(output_path);
        write_phase_gate_report(&output_path, &report).map_err(|error| {
            format!(
                "phase_gate_report_write_failed path={} error={error}",
                output_path.display()
            )
        })?;
        println!(
            "INFO phase_gate_report_written path={} overall_pass={}",
            output_path.display(),
            report.overall_pass
        );
    } else {
        let json = serde_json::to_string_pretty(&report)
            .map_err(|error| format!("phase_gate_report_serialize_failed: {error}"))?;
        println!("{json}");
    }

    Ok(report.overall_pass)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("ERROR phase_1_3_gate_runner overall_pass=false");
            ExitCode::from(1)
        }
        Err(error) => {
            eprintln!("ERROR phase_1_3_gate_runner failed: {error}");
            ExitCode::from(2)
        }
    }
}
