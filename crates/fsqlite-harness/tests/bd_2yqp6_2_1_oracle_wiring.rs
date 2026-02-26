//! Oracle wiring contract tests for bd-2yqp6.2.1.

use fsqlite_harness::differential_v2::{
    run_differential, run_differential_diagnostic, CsqliteExecutor, ExecutionEnvelope,
    FsqliteExecutor, Outcome,
};
use proptest::prelude::*;

const BEAD_ID: &str = "bd-2yqp6.2.1";
const SEED: u64 = 2_602_002_101;

fn envelope_with_csqlite_version(csqlite_version: &str) -> ExecutionEnvelope {
    ExecutionEnvelope::builder(SEED)
        .run_id(format!("{BEAD_ID}-oracle-wiring"))
        .engines("fsqlite-test", csqlite_version)
        .workload(["SELECT 1".to_owned(), "SELECT 'oracle'".to_owned()])
        .build()
}

#[test]
fn builder_default_sets_non_empty_csqlite_version_metadata() {
    let envelope = ExecutionEnvelope::builder(SEED).build();
    assert!(
        !envelope.engines.csqlite.trim().is_empty(),
        "default csqlite version metadata must be non-empty"
    );
    assert_eq!(envelope.engines.subject_identity, "frankensqlite");
    assert_eq!(envelope.engines.reference_identity, "csqlite-oracle");
}

#[test]
fn parity_mode_accepts_csqlite_oracle_executor() {
    let envelope = envelope_with_csqlite_version("3.52.0-test");
    let fsqlite = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let csqlite = CsqliteExecutor::open_in_memory().expect("csqlite open");

    let result = run_differential(&envelope, &fsqlite, &csqlite);
    assert_eq!(
        result.outcome,
        Outcome::Pass,
        "parity run should pass with a C-SQLite oracle"
    );
}

#[test]
fn parity_mode_rejects_fsqlite_self_compare_as_oracle() {
    let envelope = envelope_with_csqlite_version("3.52.0-test");
    let left = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let right = FsqliteExecutor::open_in_memory().expect("fsqlite open");

    let result = run_differential(&envelope, &left, &right);
    assert_eq!(
        result.outcome,
        Outcome::Error,
        "parity mode must reject FrankenSQLite as reference oracle"
    );
}

#[test]
fn diagnostic_mode_allows_explicit_self_compare() {
    let envelope = envelope_with_csqlite_version("3.52.0-test");
    let left = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let right = FsqliteExecutor::open_in_memory().expect("fsqlite open");

    let result = run_differential_diagnostic(&envelope, &left, &right);
    assert_eq!(
        result.outcome,
        Outcome::Pass,
        "diagnostic mode should allow explicit self-comparison"
    );
}

#[test]
fn parity_mode_rejects_blank_reference_identity_metadata() {
    let envelope = ExecutionEnvelope::builder(SEED)
        .run_id(format!("{BEAD_ID}-blank-reference-identity"))
        .engines("fsqlite-test", "3.52.0-test")
        .engine_identities("frankensqlite", "")
        .workload(["SELECT 1".to_owned()])
        .build();
    let fsqlite = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let csqlite = CsqliteExecutor::open_in_memory().expect("csqlite open");

    let result = run_differential(&envelope, &fsqlite, &csqlite);
    assert_eq!(result.outcome, Outcome::Error);
}

#[test]
fn parity_mode_rejects_non_oracle_reference_identity_metadata() {
    let envelope = ExecutionEnvelope::builder(SEED)
        .run_id(format!("{BEAD_ID}-bad-reference-identity"))
        .engines("fsqlite-test", "3.52.0-test")
        .engine_identities("frankensqlite", "sqlmodel-frankensqlite")
        .workload(["SELECT 1".to_owned()])
        .build();
    let fsqlite = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let csqlite = CsqliteExecutor::open_in_memory().expect("csqlite open");

    let result = run_differential(&envelope, &fsqlite, &csqlite);
    assert_eq!(result.outcome, Outcome::Error);
}

proptest! {
    #[test]
    fn parity_mode_rejects_blank_csqlite_version_metadata(blank in "\\s*") {
        prop_assume!(blank.trim().is_empty());
        let envelope = envelope_with_csqlite_version(&blank);
        let fsqlite = FsqliteExecutor::open_in_memory().expect("fsqlite open");
        let csqlite = CsqliteExecutor::open_in_memory().expect("csqlite open");

        let result = run_differential(&envelope, &fsqlite, &csqlite);
        prop_assert_eq!(result.outcome, Outcome::Error);
    }
}
