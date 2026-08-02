use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const BEAD_ID: &str = "bd-16e7";
const LOG_PREFIX: &str = "[REGR_GUARD]";
const REGRESSION_BASELINE_PATH: &str = "tests/regression_baseline.json";
const TRANSCRIPT_ENV: &str = "FSQLITE_REGRESSION_GUARD_TRANSCRIPT";
const CARGO_STATUS_ENV: &str = "FSQLITE_REGRESSION_GUARD_CARGO_STATUS";
const TEE_STATUS_ENV: &str = "FSQLITE_REGRESSION_GUARD_TEE_STATUS";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegressionBaseline {
    as_of_phase: String,
    total_tests: u64,
    passed: u64,
    failed: u64,
    ignored: u64,
    baseline_commit: String,
}

impl RegressionBaseline {
    fn validate(&self) -> Result<(), String> {
        if self.as_of_phase.trim().is_empty() {
            return Err("as_of_phase must not be empty".to_owned());
        }
        if self.total_tests == 0 {
            return Err("total_tests must be greater than zero".to_owned());
        }

        let accounted_tests = self
            .passed
            .checked_add(self.failed)
            .and_then(|count| count.checked_add(self.ignored))
            .ok_or_else(|| "baseline test counts overflowed u64".to_owned())?;
        if self.total_tests != accounted_tests {
            return Err(format!(
                "total_tests={} does not equal passed + failed + ignored={accounted_tests}",
                self.total_tests
            ));
        }
        if self.failed != 0 {
            return Err(format!(
                "release regression baseline must have zero failures, found {}",
                self.failed
            ));
        }
        if self.ignored != 0 {
            return Err(format!(
                "aggregate baseline cannot ratchet ignored tests above zero, found {}",
                self.ignored
            ));
        }

        let commit = self.baseline_commit.trim();
        if !(7..=40).contains(&commit.len()) || !commit.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(format!(
                "baseline_commit must be a 7-40 digit hexadecimal Git object name, found `{commit}`"
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegressionCounts {
    total_tests: u64,
    passed: u64,
    failed: u64,
    ignored: u64,
}

impl RegressionCounts {
    const fn zero() -> Self {
        Self {
            total_tests: 0,
            passed: 0,
            failed: 0,
            ignored: 0,
        }
    }

    fn checked_add(&mut self, rhs: Self) -> Result<(), String> {
        let total_tests = self
            .total_tests
            .checked_add(rhs.total_tests)
            .ok_or_else(|| "aggregate total_tests overflowed u64".to_owned())?;
        let passed = self
            .passed
            .checked_add(rhs.passed)
            .ok_or_else(|| "aggregate passed count overflowed u64".to_owned())?;
        let failed = self
            .failed
            .checked_add(rhs.failed)
            .ok_or_else(|| "aggregate failed count overflowed u64".to_owned())?;
        let ignored = self
            .ignored
            .checked_add(rhs.ignored)
            .ok_or_else(|| "aggregate ignored count overflowed u64".to_owned())?;

        *self = Self {
            total_tests,
            passed,
            failed,
            ignored,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegressionDelta {
    delta_total: i64,
    delta_passed: i64,
    delta_failed: i64,
    delta_ignored: i64,
    new_tests: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegressionReport {
    pass: bool,
    delta: RegressionDelta,
    reason: Option<String>,
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("harness crate should be nested under workspace root")
}

fn baseline_path(root: &Path) -> PathBuf {
    root.join(REGRESSION_BASELINE_PATH)
}

fn load_regression_baseline(path: &Path) -> Result<RegressionBaseline, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("unable to read baseline at {}: {error}", path.display()))?;
    let baseline = serde_json::from_slice::<RegressionBaseline>(&bytes).map_err(|error| {
        format!(
            "unable to parse baseline JSON at {}: {error}",
            path.display()
        )
    })?;
    baseline
        .validate()
        .map_err(|error| format!("invalid regression baseline at {}: {error}", path.display()))?;
    Ok(baseline)
}

fn parse_count_segment(segment: &str, label: &str) -> Option<u64> {
    let suffix = format!(" {label}");
    let value_prefix = segment.trim().strip_suffix(&suffix)?;
    let count_text = value_prefix.split_whitespace().last()?;
    count_text.parse::<u64>().ok()
}

fn parse_summary_line(line: &str) -> Option<RegressionCounts> {
    let result = line.strip_prefix("test result: ")?;
    let outcome = result.split_whitespace().next()?;
    if !matches!(outcome, "ok." | "FAILED.") {
        return None;
    }

    let mut passed = None;
    let mut failed = None;
    let mut ignored = None;

    for segment in line.split(';') {
        if passed.is_none() {
            passed = parse_count_segment(segment, "passed");
        }
        if failed.is_none() {
            failed = parse_count_segment(segment, "failed");
        }
        if ignored.is_none() {
            ignored = parse_count_segment(segment, "ignored");
        }
    }

    let passed = passed?;
    let failed = failed?;
    let ignored = ignored?;
    if (outcome == "ok." && failed != 0) || (outcome == "FAILED." && failed == 0) {
        return None;
    }
    let total_tests = passed.checked_add(failed)?.checked_add(ignored)?;

    Some(RegressionCounts {
        total_tests,
        passed,
        failed,
        ignored,
    })
}

fn cargo_target_section(line: &str) -> Option<&str> {
    if let Some(section) = line.strip_prefix("     Running ") {
        if !section.is_empty() && section.contains(" (") && section.ends_with(')') {
            return Some(section);
        }
    }

    let section = line.strip_prefix("   Doc-tests ")?;
    (!section.trim().is_empty() && section == section.trim_end()).then_some(section)
}

fn parse_workspace_test_counts(output: &str) -> Result<RegressionCounts, String> {
    if output.contains('\u{1b}') {
        return Err("workspace transcript contains ANSI escape sequences".to_owned());
    }

    let mut totals = RegressionCounts::zero();
    let mut active_section: Option<String> = None;
    let mut active_summary: Option<RegressionCounts> = None;

    for line in output.lines() {
        if let Some(section_header) = cargo_target_section(line) {
            if let Some(section) = active_section.take() {
                let summary = active_summary.take().ok_or_else(|| {
                    format!("cargo target section `{section}` had no test-result summary")
                })?;
                totals.checked_add(summary)?;
            }
            active_section = Some(section_header.to_owned());
            continue;
        }

        if line.starts_with("test result: ") {
            let parsed = parse_summary_line(line)
                .ok_or_else(|| format!("malformed cargo test summary line: {line}"))?;
            if active_section.is_none() {
                return Err(format!(
                    "cargo test summary appeared outside a target section: {line}"
                ));
            }
            // Subprocess helpers can emit their own summaries into a parent
            // target's captured output. The outer harness summary is last and
            // is therefore the only authoritative count for this section.
            active_summary = Some(parsed);
        }
    }

    let section = active_section
        .ok_or_else(|| "no cargo test target sections were found in output".to_owned())?;
    let summary = active_summary
        .ok_or_else(|| format!("cargo target section `{section}` had no test-result summary"))?;
    totals.checked_add(summary)?;

    Ok(totals)
}

fn parse_required_status(name: &str) -> Result<i32, String> {
    let value = std::env::var(name).map_err(|error| format!("missing {name}: {error}"))?;
    value
        .parse::<i32>()
        .map_err(|error| format!("invalid {name} value `{value}`: {error}"))
}

fn validate_process_statuses(cargo_status: i32, tee_status: i32) -> Result<(), String> {
    if cargo_status != 0 {
        return Err(format!(
            "canonical workspace cargo test exited {cargo_status}"
        ));
    }
    if tee_status != 0 {
        return Err(format!("workspace transcript tee exited {tee_status}"));
    }
    Ok(())
}

fn as_i64(value: i128) -> i64 {
    match i64::try_from(value) {
        Ok(v) => v,
        Err(_) => {
            if value.is_negative() {
                i64::MIN
            } else {
                i64::MAX
            }
        }
    }
}

fn compare_against_baseline(
    baseline: &RegressionBaseline,
    actual: &RegressionCounts,
) -> RegressionReport {
    let delta_total = i128::from(actual.total_tests) - i128::from(baseline.total_tests);
    let delta_passed = i128::from(actual.passed) - i128::from(baseline.passed);
    let delta_failed = i128::from(actual.failed) - i128::from(baseline.failed);
    let delta_ignored = i128::from(actual.ignored) - i128::from(baseline.ignored);

    let delta = RegressionDelta {
        delta_total: as_i64(delta_total),
        delta_passed: as_i64(delta_passed),
        delta_failed: as_i64(delta_failed),
        delta_ignored: as_i64(delta_ignored),
        new_tests: as_i64(delta_total),
    };

    let mut reasons = Vec::new();
    if actual.failed > baseline.failed {
        reasons.push(format!(
            "failed increased from {} to {}",
            baseline.failed, actual.failed
        ));
    }
    if actual.passed < baseline.passed {
        reasons.push(format!(
            "passed decreased from {} to {}",
            baseline.passed, actual.passed
        ));
    }
    if actual.ignored > baseline.ignored {
        reasons.push(format!(
            "ignored increased from {} to {}",
            baseline.ignored, actual.ignored
        ));
    }
    if actual.total_tests < baseline.total_tests {
        reasons.push(format!(
            "total tests decreased from {} to {}",
            baseline.total_tests, actual.total_tests
        ));
    }

    let pass = reasons.is_empty();
    let reason = if pass { None } else { Some(reasons.join("; ")) };

    RegressionReport {
        pass,
        delta,
        reason,
    }
}

fn extract_failed_tests(output: &str) -> Vec<String> {
    let mut failed = Vec::new();
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("test ") && trimmed.ends_with(" ... FAILED") {
            failed.push(trimmed.to_owned());
        }
    }
    failed
}

#[test]
fn test_regression_guard_parses_cargo_output() {
    let sample = r"
     Running unittests src/lib.rs (target/debug/deps/example-a1)
test result: ok. 4 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s
     Running tests/integration.rs (target/debug/deps/integration-b2)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";

    let counts = parse_workspace_test_counts(sample)
        .expect("sample output should parse into aggregate regression counts");
    assert_eq!(
        counts.total_tests, 7,
        "bead_id={BEAD_ID} case=parse_output_total"
    );
    assert_eq!(
        counts.passed, 6,
        "bead_id={BEAD_ID} case=parse_output_passed"
    );
    assert_eq!(
        counts.failed, 0,
        "bead_id={BEAD_ID} case=parse_output_failed"
    );
    assert_eq!(
        counts.ignored, 1,
        "bead_id={BEAD_ID} case=parse_output_ignored"
    );
}

#[test]
fn test_regression_guard_uses_last_summary_per_target_section() {
    let sample = r"
     Running tests/parent.rs (target/debug/deps/parent-a1)
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 4 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s
   Doc-tests example
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";

    let counts = parse_workspace_test_counts(sample)
        .expect("last summary in each target section should be authoritative");
    assert_eq!(counts.total_tests, 7);
    assert_eq!(counts.passed, 6);
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.ignored, 1);
}

#[test]
fn test_regression_guard_rejects_unframed_malformed_and_colored_summaries() {
    let unframed = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out";
    assert!(
        parse_workspace_test_counts(unframed)
            .expect_err("summary without target section must fail")
            .contains("outside a target section")
    );

    let malformed = "Running tests/example.rs (example)\ntest result: ok. not-a-count passed";
    assert!(
        parse_workspace_test_counts(malformed)
            .expect_err("malformed summary must fail")
            .contains("malformed cargo test summary")
    );

    let missing = "Running tests/example.rs (example)";
    assert!(
        parse_workspace_test_counts(missing)
            .expect_err("missing summary must fail")
            .contains("had no test-result summary")
    );

    let colored = "\u{1b}[32mRunning tests/example.rs (example)\u{1b}[0m";
    assert!(
        parse_workspace_test_counts(colored)
            .expect_err("colored transcript must fail closed")
            .contains("ANSI escape")
    );

    let invalid_outcome = "     Running tests/example.rs (example)\ntest result: MAYBE. 1 passed; 0 failed; 0 ignored";
    assert!(
        parse_workspace_test_counts(invalid_outcome)
            .expect_err("unknown libtest outcome must fail")
            .contains("malformed cargo test summary")
    );
}

#[test]
fn test_regression_guard_ignores_unanchored_subprocess_noise() {
    let sample = r"
     Running tests/outer.rs (target/debug/deps/outer-a1)
helper: test result: ok. 90 passed; 0 failed; 0 ignored
  Running tests/not-a-cargo-header.rs (helper)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
";

    let counts = parse_workspace_test_counts(sample)
        .expect("unanchored subprocess output must not alter Cargo target counts");
    assert_eq!(counts.total_tests, 2);
    assert_eq!(counts.passed, 2);
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.ignored, 0);
}

#[test]
fn test_regression_guard_requires_successful_cargo_and_tee_statuses() {
    assert_eq!(validate_process_statuses(0, 0), Ok(()));
    assert!(
        validate_process_statuses(1, 0)
            .expect_err("nonzero cargo status must fail")
            .contains("cargo test exited 1")
    );
    assert!(
        validate_process_statuses(0, 7)
            .expect_err("nonzero tee status must fail")
            .contains("tee exited 7")
    );
}

#[test]
fn test_regression_guard_baseline_schema_rejects_unknown_fields() {
    let baseline = r#"{
        "as_of_phase": "checkpoint_1",
        "total_tests": 1,
        "passed": 1,
        "failed": 0,
        "ignored": 0,
        "baseline_commit": "deadbeef",
        "unexpected": true
    }"#;
    let error = serde_json::from_str::<RegressionBaseline>(baseline)
        .expect_err("unknown baseline fields must fail closed");
    assert!(error.to_string().contains("unknown field `unexpected`"));
}

#[test]
fn test_regression_guard_baseline_validation_fails_closed() {
    let valid = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 3,
        passed: 3,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
    };
    assert_eq!(valid.validate(), Ok(()));

    let mut inconsistent = valid.clone();
    inconsistent.total_tests = 4;
    assert!(
        inconsistent
            .validate()
            .expect_err("inconsistent totals must fail")
            .contains("does not equal")
    );

    let mut failing = valid.clone();
    failing.total_tests = 4;
    failing.failed = 1;
    assert!(
        failing
            .validate()
            .expect_err("a failing release baseline must fail")
            .contains("zero failures")
    );

    let mut ignored = valid.clone();
    ignored.total_tests = 4;
    ignored.ignored = 1;
    assert!(
        ignored
            .validate()
            .expect_err("an ignored-test ratchet must fail")
            .contains("cannot ratchet ignored tests")
    );

    let mut bad_commit = valid;
    bad_commit.baseline_commit = "not-a-commit".to_owned();
    assert!(
        bad_commit
            .validate()
            .expect_err("invalid commit provenance must fail")
            .contains("hexadecimal Git object name")
    );
}

#[test]
fn test_regression_guard_detects_failure() {
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 5_319,
        passed: 5_319,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
    };
    let actual = RegressionCounts {
        total_tests: 5_319,
        passed: 5_317,
        failed: 2,
        ignored: 0,
    };

    let report = compare_against_baseline(&baseline, &actual);
    assert!(
        !report.pass,
        "bead_id={BEAD_ID} case=detect_failure_report_must_fail"
    );
    let reason = report.reason.unwrap_or_default();
    assert!(
        reason.contains("failed increased"),
        "bead_id={BEAD_ID} case=detect_failure_reason reason={reason}"
    );
}

#[test]
fn test_regression_guard_baseline_comparison() {
    let baseline = RegressionBaseline {
        as_of_phase: "checkpoint_1".to_owned(),
        total_tests: 5_319,
        passed: 5_319,
        failed: 0,
        ignored: 0,
        baseline_commit: "deadbeef".to_owned(),
    };
    let actual = RegressionCounts {
        total_tests: 5_322,
        passed: 5_322,
        failed: 0,
        ignored: 0,
    };

    let report = compare_against_baseline(&baseline, &actual);
    assert!(
        report.pass,
        "bead_id={BEAD_ID} case=baseline_compare_should_pass report={report:?}"
    );
    assert_eq!(
        report.delta.new_tests, 3,
        "bead_id={BEAD_ID} case=baseline_compare_new_tests"
    );
    assert_eq!(
        report.delta.delta_failed, 0,
        "bead_id={BEAD_ID} case=baseline_compare_failed_delta"
    );
}

#[test]
#[ignore = "Validates an externally captured canonical workspace transcript against the regression baseline"]
fn phase5_regression_guard_full_workspace_against_baseline() -> Result<(), String> {
    let root = repo_root();
    let baseline_file = baseline_path(&root);
    let baseline = load_regression_baseline(&baseline_file)
        .map_err(|error| format!("bead_id={BEAD_ID} case=load_baseline_failed error={error}"))?;
    let transcript_path = std::env::var(TRANSCRIPT_ENV)
        .map(PathBuf::from)
        .map_err(|error| format!("bead_id={BEAD_ID} case=missing_transcript error={error}"))?;
    let cargo_status = parse_required_status(CARGO_STATUS_ENV)
        .map_err(|error| format!("bead_id={BEAD_ID} case=cargo_status error={error}"))?;
    let tee_status = parse_required_status(TEE_STATUS_ENV)
        .map_err(|error| format!("bead_id={BEAD_ID} case=tee_status error={error}"))?;
    let transcript = fs::read_to_string(&transcript_path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=read_transcript path={} error={error}",
            transcript_path.display()
        )
    })?;

    eprintln!(
        "{LOG_PREFIX}[phase={}][step=validate_transcript] path={} cargo_status={} tee_status={}",
        baseline.as_of_phase,
        transcript_path.display(),
        cargo_status,
        tee_status
    );

    if let Err(error) = validate_process_statuses(cargo_status, tee_status) {
        for failed in extract_failed_tests(&transcript) {
            eprintln!(
                "{LOG_PREFIX}[phase={}][step=failures] test_name=\"{}\"",
                baseline.as_of_phase, failed
            );
        }
        return Err(format!(
            "{LOG_PREFIX}[phase={}][result=FAIL] {error}",
            baseline.as_of_phase
        ));
    }

    let counts = parse_workspace_test_counts(&transcript).map_err(|error| {
        format!("bead_id={BEAD_ID} case=parse_workspace_output_failed error={error}")
    })?;

    eprintln!(
        "{LOG_PREFIX}[phase={}][step=parse_results] total={} passed={} failed={} ignored={}",
        baseline.as_of_phase, counts.total_tests, counts.passed, counts.failed, counts.ignored
    );

    let report = compare_against_baseline(&baseline, &counts);
    eprintln!(
        "{LOG_PREFIX}[phase={}][step=compare_baseline] delta_passed={} delta_failed={} new_tests={}",
        baseline.as_of_phase,
        report.delta.delta_passed,
        report.delta.delta_failed,
        report.delta.new_tests
    );

    if report.pass {
        eprintln!(
            "{LOG_PREFIX}[phase={}][result=PASS] Aggregate baseline counts preserved against commit {}",
            baseline.as_of_phase, baseline.baseline_commit
        );
        return Ok(());
    }

    for failed in extract_failed_tests(&transcript) {
        eprintln!(
            "{LOG_PREFIX}[phase={}][step=failures] test_name=\"{}\"",
            baseline.as_of_phase, failed
        );
    }

    let reason = report
        .reason
        .unwrap_or_else(|| "unknown regression detected".to_owned());
    Err(format!(
        "{LOG_PREFIX}[phase={}][result=FAIL] {reason}; baseline_commit={} cargo_status={}",
        baseline.as_of_phase, baseline.baseline_commit, cargo_status
    ))
}
