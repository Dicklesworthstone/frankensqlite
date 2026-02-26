//! Anti-regression tests for concurrent-mode defaults (bd-2yqp6.6.5).

use std::path::PathBuf;

use fsqlite_e2e::fairness;
use fsqlite_e2e::fsqlite_executor::{run_oplog_fsqlite, FsqliteExecConfig};
use fsqlite_e2e::oplog::preset_commutative_inserts_disjoint_keys;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-2yqp6.6.5";
const SCENARIO_ID: &str = "CONCURRENT-DEFAULTS-F5";
const SEED: u64 = 3520;

#[test]
fn fsqlite_exec_config_default_keeps_concurrent_mode_on() {
    let cfg = FsqliteExecConfig::default();
    assert!(
        cfg.concurrent_mode,
        "concurrent_mode default must remain true in FsqliteExecConfig"
    );
}

#[test]
fn fairness_benchmark_settings_default_keeps_concurrent_mode_on() {
    let settings = fairness::benchmark_settings();
    assert!(
        settings.concurrent_mode,
        "concurrent_mode default must remain true in benchmark_settings()"
    );
}

#[test]
fn deterministic_file_backed_smoke_uses_concurrent_mode_by_default() {
    let temp = tempdir().expect("tempdir");
    let db_path: PathBuf = temp.path().join("f5-default-on.db");
    let oplog = preset_commutative_inserts_disjoint_keys(SCENARIO_ID, SEED, 1, 4);

    let report = run_oplog_fsqlite(&db_path, &oplog, &FsqliteExecConfig::default())
        .expect("run_oplog_fsqlite default config");

    assert!(
        report.error.is_none(),
        "unexpected execution error with default config: {:?}",
        report.error
    );
    let notes = report.correctness.notes.unwrap_or_default();
    assert!(
        notes.contains("mode=concurrent (MVCC)"),
        "expected default mode note to indicate concurrent MVCC, got: {notes}"
    );
}

#[test]
fn explicit_opt_out_is_respected_without_flipping_default() {
    let temp = tempdir().expect("tempdir");
    let db_path: PathBuf = temp.path().join("f5-explicit-off.db");
    let oplog = preset_commutative_inserts_disjoint_keys(SCENARIO_ID, SEED, 1, 3);

    let cfg = FsqliteExecConfig {
        concurrent_mode: false,
        ..FsqliteExecConfig::default()
    };
    let report =
        run_oplog_fsqlite(&db_path, &oplog, &cfg).expect("run_oplog_fsqlite explicit off config");
    let notes = report.correctness.notes.unwrap_or_default();
    assert!(
        notes.contains("mode=single-writer (serialized)"),
        "expected explicit opt-out note, got: {notes}"
    );
}

#[test]
fn bead_metadata_constants_are_stable_for_replay() {
    assert_eq!(BEAD_ID, "bd-2yqp6.6.5");
    assert_eq!(SCENARIO_ID, "CONCURRENT-DEFAULTS-F5");
    assert_eq!(SEED, 3520);
}
