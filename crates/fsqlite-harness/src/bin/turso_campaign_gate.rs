use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use fsqlite_harness::ci_coverage_gate::{
    CampaignPromotionOutcome, TursoCampaignGateInput, evaluate_turso_campaign_gate,
};

#[derive(Debug)]
struct Config {
    input: PathBuf,
    output: PathBuf,
}

enum ParseResult {
    Run(Config),
    Help,
}

impl Config {
    fn parse() -> Result<ParseResult, String> {
        let mut input = None;
        let mut output = None;
        let mut args = env::args().skip(1);
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--input" => {
                    input = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "missing value for --input".to_owned())?,
                    ));
                }
                "--output" => {
                    output = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "missing value for --output".to_owned())?,
                    ));
                }
                "--help" | "-h" => return Ok(ParseResult::Help),
                argument => return Err(format!("unknown argument: {argument}")),
            }
        }

        Ok(ParseResult::Run(Self {
            input: input.ok_or_else(|| "--input is required".to_owned())?,
            output: output.ok_or_else(|| "--output is required".to_owned())?,
        }))
    }
}

fn print_help() {
    println!(
        "\
turso_campaign_gate - evaluate retained Turso campaign evidence

USAGE:
  cargo run -p fsqlite-harness --bin turso_campaign_gate -- \
    --input <campaign-input.json> --output <scorecard.json>

For valid input, the command always writes a scorecard. It exits non-zero when promotion is held."
    );
}

fn read_input(path: &Path) -> Result<TursoCampaignGateInput, String> {
    let json = fs::read_to_string(path).map_err(|error| {
        format!(
            "campaign input read failed path={}: {error}",
            path.display()
        )
    })?;
    TursoCampaignGateInput::from_json(&json)
        .map_err(|error| format!("campaign input invalid path={}: {error}", path.display()))
}

fn run(config: &Config) -> Result<ExitCode, String> {
    let input = read_input(&config.input)?;
    let scorecard = evaluate_turso_campaign_gate(&input);
    let json = scorecard
        .to_json()
        .map_err(|error| format!("campaign scorecard serialization failed: {error}"))?;
    if let Some(parent) = config
        .output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "campaign scorecard directory create failed path={}: {error}",
                parent.display()
            )
        })?;
    }
    fs::write(&config.output, json).map_err(|error| {
        format!(
            "campaign scorecard write failed path={}: {error}",
            config.output.display()
        )
    })?;

    print!("{}", scorecard.render_bounded_summary());
    Ok(if scorecard.outcome == CampaignPromotionOutcome::Promote {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn main() -> ExitCode {
    match Config::parse() {
        Ok(ParseResult::Help) => {
            print_help();
            ExitCode::SUCCESS
        }
        Ok(ParseResult::Run(config)) => match run(&config) {
            Ok(code) => code,
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
