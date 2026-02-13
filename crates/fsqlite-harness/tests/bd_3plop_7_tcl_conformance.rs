use std::path::PathBuf;

use fsqlite_harness::tcl_conformance::{
    build_validated_tcl_harness_suite, execute_tcl_harness_suite, parse_testrunner_counts,
    TclExecutionMode, TclExecutionOptions, TclHarnessOutcome, BEAD_ID,
};

#[test]
fn canonical_suite_validates() {
    let suite =
        build_validated_tcl_harness_suite().expect("canonical TCL harness suite should validate");
    assert_eq!(suite.bead_id, BEAD_ID);
    assert!(
        suite.scenarios.len() >= 2,
        "bead_id={BEAD_ID} expected at least two deterministic scenarios"
    );
}

#[test]
fn testrunner_summary_parser_extracts_counts_and_skips() {
    let sample = "\
1 failures:
FAILED: quick.test
3 jobs skipped due to prior failures
0 errors out of 42 tests in 00:00:07 linux
";

    let parsed = parse_testrunner_counts(sample).expect("summary line should parse");
    assert_eq!(parsed.errors, 0);
    assert_eq!(parsed.tests, 42);
    assert_eq!(parsed.skipped_jobs, 3);
}

#[test]
fn dry_run_execution_reports_skipped_without_side_effects() {
    let suite =
        build_validated_tcl_harness_suite().expect("canonical TCL harness suite should validate");
    let summary = execute_tcl_harness_suite(
        &suite,
        TclExecutionOptions {
            mode: TclExecutionMode::DryRun,
            timeout_secs: 60,
            max_scenarios: Some(1),
            runner_override: None,
            run_id_override: Some("bd-3plop-7-test-dry-run".to_owned()),
        },
    )
    .expect("dry-run summary should succeed");

    assert_eq!(summary.mode, TclExecutionMode::DryRun);
    assert_eq!(summary.total_scenarios, 1);
    assert_eq!(summary.skipped_scenarios, 1);
    assert_eq!(summary.failed_scenarios, 0);
    assert_eq!(summary.error_scenarios, 0);
    assert_eq!(summary.timeout_scenarios, 0);
    assert_eq!(summary.results.len(), 1);
    assert_eq!(summary.results[0].outcome, TclHarnessOutcome::Skipped);
    assert_eq!(summary.results[0].reason.as_deref(), Some("dry_run_mode"));
}

#[test]
fn execute_mode_with_missing_runner_is_graceful_skip() {
    let suite =
        build_validated_tcl_harness_suite().expect("canonical TCL harness suite should validate");
    let summary = execute_tcl_harness_suite(
        &suite,
        TclExecutionOptions {
            mode: TclExecutionMode::Execute,
            timeout_secs: 1,
            max_scenarios: Some(1),
            runner_override: Some(PathBuf::from("/tmp/fsqlite-does-not-exist/testrunner.tcl")),
            run_id_override: Some("bd-3plop-7-test-missing-runner".to_owned()),
        },
    )
    .expect("execute summary should succeed when runner missing");

    assert_eq!(summary.total_scenarios, 1);
    assert_eq!(summary.skipped_scenarios, 1);
    assert_eq!(summary.results[0].outcome, TclHarnessOutcome::Skipped);
    assert!(
        summary.results[0]
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("runner_not_found")),
        "bead_id={BEAD_ID} expected runner_not_found skip reason"
    );
}
