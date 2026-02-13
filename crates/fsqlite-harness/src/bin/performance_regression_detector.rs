use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use fsqlite_harness::performance_regression_detector::{
    BEAD_ID, BenchmarkSample, RegressionSeverity, RegressionTolerance,
    evaluate_candidate_against_baseline, load_baseline_samples, write_detection_report,
};

#[derive(Debug)]
struct CliConfig {
    baseline_path: PathBuf,
    candidate_path: PathBuf,
    output_path: Option<PathBuf>,
    tolerance: RegressionTolerance,
    fail_on: RegressionSeverity,
}

fn print_help() {
    let help = "\
performance_regression_detector — noise-aware baseline regression gate (bd-mblr.7.3.2)

USAGE:
    cargo run -p fsqlite-harness --bin performance_regression_detector -- [OPTIONS]

OPTIONS:
    --baseline <PATH>                  Baseline sample JSON file (required)
    --candidate <PATH>                 Candidate sample JSON file (required)
    --output <PATH>                    Write report JSON to file (stdout when omitted)
    --warning-latency-ratio <f64>      Warning latency ratio threshold (default 1.10)
    --critical-latency-ratio <f64>     Critical latency ratio threshold (default 1.25)
    --warning-throughput-drop <f64>    Warning throughput drop ratio (default 0.10)
    --critical-throughput-drop <f64>   Critical throughput drop ratio (default 0.20)
    --fail-on <LEVEL>                  none|info|warning|critical (default critical)
    -h, --help                         Show this help
";
    println!("{help}");
}

fn parse_fail_on(value: &str) -> Option<RegressionSeverity> {
    match value {
        "none" => Some(RegressionSeverity::None),
        "info" => Some(RegressionSeverity::Info),
        "warning" => Some(RegressionSeverity::Warning),
        "critical" => Some(RegressionSeverity::Critical),
        _ => None,
    }
}

fn parse_args(args: &[String]) -> Result<CliConfig, String> {
    let mut baseline_path: Option<PathBuf> = None;
    let mut candidate_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut tolerance = RegressionTolerance::default();
    let mut fail_on = RegressionSeverity::Critical;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--baseline" => {
                index += 1;
                if index >= args.len() {
                    return Err("--baseline requires a value".to_owned());
                }
                baseline_path = Some(PathBuf::from(&args[index]));
            }
            "--candidate" => {
                index += 1;
                if index >= args.len() {
                    return Err("--candidate requires a value".to_owned());
                }
                candidate_path = Some(PathBuf::from(&args[index]));
            }
            "--output" => {
                index += 1;
                if index >= args.len() {
                    return Err("--output requires a value".to_owned());
                }
                output_path = Some(PathBuf::from(&args[index]));
            }
            "--warning-latency-ratio" => {
                index += 1;
                if index >= args.len() {
                    return Err("--warning-latency-ratio requires a value".to_owned());
                }
                tolerance.warning_latency_ratio = args[index].parse::<f64>().map_err(|_| {
                    format!("invalid --warning-latency-ratio value: {}", args[index])
                })?;
            }
            "--critical-latency-ratio" => {
                index += 1;
                if index >= args.len() {
                    return Err("--critical-latency-ratio requires a value".to_owned());
                }
                tolerance.critical_latency_ratio = args[index].parse::<f64>().map_err(|_| {
                    format!("invalid --critical-latency-ratio value: {}", args[index])
                })?;
            }
            "--warning-throughput-drop" => {
                index += 1;
                if index >= args.len() {
                    return Err("--warning-throughput-drop requires a value".to_owned());
                }
                tolerance.warning_throughput_drop_ratio =
                    args[index].parse::<f64>().map_err(|_| {
                        format!("invalid --warning-throughput-drop value: {}", args[index])
                    })?;
            }
            "--critical-throughput-drop" => {
                index += 1;
                if index >= args.len() {
                    return Err("--critical-throughput-drop requires a value".to_owned());
                }
                tolerance.critical_throughput_drop_ratio =
                    args[index].parse::<f64>().map_err(|_| {
                        format!("invalid --critical-throughput-drop value: {}", args[index])
                    })?;
            }
            "--fail-on" => {
                index += 1;
                if index >= args.len() {
                    return Err("--fail-on requires a value".to_owned());
                }
                let normalized = args[index].to_ascii_lowercase();
                fail_on = parse_fail_on(&normalized).ok_or_else(|| {
                    format!(
                        "invalid --fail-on value: {} (expected none|info|warning|critical)",
                        args[index]
                    )
                })?;
            }
            "-h" | "--help" => {
                print_help();
                return Err(String::new());
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        index += 1;
    }

    Ok(CliConfig {
        baseline_path: baseline_path.ok_or_else(|| "--baseline is required".to_owned())?,
        candidate_path: candidate_path.ok_or_else(|| "--candidate is required".to_owned())?,
        output_path,
        tolerance,
        fail_on,
    })
}

fn load_candidate_sample(path: &PathBuf) -> Result<BenchmarkSample, String> {
    let payload = std::fs::read(path).map_err(|error| {
        format!(
            "candidate_read_failed path={} error={error}",
            path.display()
        )
    })?;
    serde_json::from_slice::<BenchmarkSample>(&payload).map_err(|error| {
        format!(
            "candidate_parse_failed path={} error={error}",
            path.display()
        )
    })
}

fn run(args: &[String]) -> Result<RegressionSeverity, String> {
    let config = parse_args(args)?;
    let baseline_samples = load_baseline_samples(&config.baseline_path)?;
    let candidate = load_candidate_sample(&config.candidate_path)?;
    let report =
        evaluate_candidate_against_baseline(&baseline_samples, &candidate, &config.tolerance)?;
    let severity = report.assessment.severity;

    if let Some(path) = &config.output_path {
        write_detection_report(path, &report)?;
    } else {
        let payload = serde_json::to_string_pretty(&report)
            .map_err(|error| format!("report_serialize_failed: {error}"))?;
        println!("{payload}");
    }

    if config.fail_on != RegressionSeverity::None && severity >= config.fail_on {
        return Ok(severity);
    }
    Ok(RegressionSeverity::None)
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match run(&args) {
        Ok(RegressionSeverity::None) => ExitCode::SUCCESS,
        Ok(severity) => {
            eprintln!(
                "ERROR bead_id={BEAD_ID} regression severity={severity} exceeded fail-on threshold"
            );
            ExitCode::from(1)
        }
        Err(error) if error.is_empty() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ERROR bead_id={BEAD_ID} performance_regression_detector failed: {error}");
            ExitCode::from(2)
        }
    }
}
