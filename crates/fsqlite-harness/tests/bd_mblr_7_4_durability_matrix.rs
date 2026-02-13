//! Compliance tests for bd-mblr.7.4 durability matrix foundation.

use std::collections::BTreeSet;

use fsqlite_harness::durability_matrix::{
    DEFAULT_ROOT_SEED, DurabilityExecutionMode, DurabilityExecutionOptions, DurabilityMatrix,
    OperatingSystem, build_validated_durability_matrix, execute_durability_matrix,
    render_operator_workflow,
};

const BEAD_ID: &str = "bd-mblr.7.4";
const LOG_STANDARD_REF: &str = "bd-1fpm";

#[test]
fn canonical_durability_matrix_is_valid_and_cross_platform() {
    let matrix = build_validated_durability_matrix(DEFAULT_ROOT_SEED)
        .expect("durability matrix should validate");

    let os_coverage: BTreeSet<_> = matrix.environments.iter().map(|env| env.os).collect();
    assert!(
        os_coverage.contains(&OperatingSystem::Linux),
        "bead_id={BEAD_ID} expected linux coverage"
    );
    assert!(
        os_coverage.contains(&OperatingSystem::MacOs),
        "bead_id={BEAD_ID} expected macOS coverage"
    );
    assert!(
        os_coverage.contains(&OperatingSystem::Windows),
        "bead_id={BEAD_ID} expected windows coverage"
    );
}

#[test]
fn rendered_workflow_contains_seeded_recovery_contracts() {
    let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
    let workflow = render_operator_workflow(&matrix);

    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase=workflow_render seed={DEFAULT_ROOT_SEED} reference={LOG_STANDARD_REF}"
    );

    assert!(
        workflow.contains("durability_matrix bead_id=bd-mblr.7.4"),
        "bead_id={BEAD_ID} workflow missing header"
    );
    assert!(
        workflow.contains("id=REC-1"),
        "bead_id={BEAD_ID} workflow missing REC-1 scenario"
    );
    assert!(
        workflow.contains("id=env-linux-ext4-nightly"),
        "bead_id={BEAD_ID} workflow missing primary linux environment"
    );
    assert!(
        workflow.contains("probes:"),
        "bead_id={BEAD_ID} workflow missing probe section"
    );
}

#[test]
fn dry_run_execution_summary_is_deterministic_and_non_failing() {
    let matrix = build_validated_durability_matrix(DEFAULT_ROOT_SEED)
        .expect("durability matrix should validate");
    let summary = execute_durability_matrix(&matrix, DurabilityExecutionOptions::default())
        .expect("dry-run execution summary should succeed");

    assert_eq!(summary.mode, DurabilityExecutionMode::DryRun);
    assert_eq!(summary.total_probes, matrix.probes.len());
    assert_eq!(summary.passed_probes, 0);
    assert_eq!(summary.failed_probes, 0);
    assert_eq!(summary.timeout_probes, 0);
    assert_eq!(summary.error_probes, 0);
    assert_eq!(summary.skipped_probes, matrix.probes.len());
}
