//! Integration tests for bd-mblr.7.8 — Determinism Watchdog Across Toolchains.
//!
//! Tests the watchdog orchestrator that ties together the toolchain determinism
//! matrix (bd-mblr.7.8.1) and cross-toolchain runner (bd-mblr.7.8.2).

use fsqlite_harness::toolchain_determinism::{
    DeterminismMatrix, DeterminismRunner, WatchdogConfig, WatchdogReport, WatchdogVerdict,
    build_canonical_corpus, canonical_probes, canonical_toolchains,
    compute_determinism_coverage, load_watchdog_report, run_watchdog, write_watchdog_report,
    WATCHDOG_BEAD_ID,
};

const BEAD_ID: &str = "bd-mblr.7.8";

// ---------------------------------------------------------------------------
// Watchdog — full pipeline
// ---------------------------------------------------------------------------

#[test]
fn watchdog_runs_canonical_matrix() {
    let config = WatchdogConfig {
        root_seed: 0xAAAA_0001,
        ..Default::default()
    };
    let report = run_watchdog(&config);

    assert_eq!(
        report.bead_id, WATCHDOG_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        report.schema_version, 1,
        "bead_id={BEAD_ID} case=schema_version"
    );
    assert!(
        report.session.probe_count > 0,
        "bead_id={BEAD_ID} case=probes_nonzero"
    );
    assert!(
        report.session.toolchain_count > 0,
        "bead_id={BEAD_ID} case=toolchains_nonzero"
    );
}

#[test]
fn watchdog_passes_with_default_config() {
    let config = WatchdogConfig::default();
    let report = run_watchdog(&config);

    // Canonical matrix in local mode should pass (all deterministic)
    assert_ne!(
        report.verdict,
        WatchdogVerdict::Fail,
        "bead_id={BEAD_ID} case=default_not_fail"
    );
}

#[test]
fn watchdog_coverage_is_complete() {
    let config = WatchdogConfig::default();
    let report = run_watchdog(&config);

    assert!(
        report.coverage.toolchain_count > 0,
        "bead_id={BEAD_ID} case=toolchain_count"
    );
    assert!(
        report.coverage.probe_count > 0,
        "bead_id={BEAD_ID} case=probe_count"
    );
    assert!(
        report.coverage.total_combinations > 0,
        "bead_id={BEAD_ID} case=total_combinations"
    );
    assert!(
        !report.coverage.subsystems_covered.is_empty(),
        "bead_id={BEAD_ID} case=subsystems_covered"
    );
}

// ---------------------------------------------------------------------------
// Verdict logic
// ---------------------------------------------------------------------------

#[test]
fn watchdog_verdict_display() {
    assert_eq!(WatchdogVerdict::Pass.to_string(), "PASS");
    assert_eq!(WatchdogVerdict::Warning.to_string(), "WARNING");
    assert_eq!(WatchdogVerdict::Fail.to_string(), "FAIL");
}

#[test]
fn watchdog_tracks_probe_failures() {
    let config = WatchdogConfig {
        root_seed: 0xBBBB_0002,
        ..Default::default()
    };
    let report = run_watchdog(&config);

    // probe_failures should be a non-negative count
    assert!(
        report.probe_failures <= report.session.probe_count,
        "bead_id={BEAD_ID} case=failures_bounded"
    );
}

// ---------------------------------------------------------------------------
// Report serialization
// ---------------------------------------------------------------------------

#[test]
fn watchdog_report_json_roundtrip() {
    let config = WatchdogConfig {
        root_seed: 0xCCCC_0003,
        ..Default::default()
    };
    let report = run_watchdog(&config);

    let json = report.to_json().expect("serialize");
    let parsed = WatchdogReport::from_json(&json).expect("parse");

    assert_eq!(parsed.bead_id, report.bead_id);
    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.probe_failures, report.probe_failures);
}

#[test]
fn watchdog_report_file_roundtrip() {
    let config = WatchdogConfig {
        root_seed: 0xDDDD_0004,
        ..Default::default()
    };
    let report = run_watchdog(&config);

    let dir = std::env::temp_dir().join("fsqlite-watchdog-test");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("watchdog-test.json");

    write_watchdog_report(&path, &report).expect("write");
    let loaded = load_watchdog_report(&path).expect("load");

    assert_eq!(loaded.verdict, report.verdict);
    assert_eq!(loaded.probe_failures, report.probe_failures);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

// ---------------------------------------------------------------------------
// Triage
// ---------------------------------------------------------------------------

#[test]
fn watchdog_triage_line_contains_key_info() {
    let config = WatchdogConfig::default();
    let report = run_watchdog(&config);
    let line = report.triage_line();

    assert!(line.contains("probes"), "bead_id={BEAD_ID}");
    assert!(line.contains("toolchains"), "bead_id={BEAD_ID}");
}

#[test]
fn watchdog_summary_is_nonempty() {
    let config = WatchdogConfig::default();
    let report = run_watchdog(&config);

    assert!(
        !report.summary.is_empty(),
        "bead_id={BEAD_ID} case=summary_nonempty"
    );
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

#[test]
fn watchdog_is_deterministic() {
    let config = WatchdogConfig {
        root_seed: 0xEEEE_0005,
        ..Default::default()
    };
    let r1 = run_watchdog(&config);
    let r2 = run_watchdog(&config);

    assert_eq!(
        r1.verdict, r2.verdict,
        "bead_id={BEAD_ID} case=deterministic_verdict"
    );
    assert_eq!(
        r1.probe_failures, r2.probe_failures,
        "bead_id={BEAD_ID} case=deterministic_failures"
    );
}

// ---------------------------------------------------------------------------
// Child bead integration
// ---------------------------------------------------------------------------

#[test]
fn canonical_matrix_is_valid() {
    let matrix = DeterminismMatrix::canonical(0x1234);
    let errors = matrix.validate();
    assert!(
        errors.is_empty(),
        "bead_id={BEAD_ID} case=matrix_valid errors={errors:?}"
    );
}

#[test]
fn canonical_toolchains_nonempty() {
    let toolchains = canonical_toolchains();
    assert!(
        !toolchains.is_empty(),
        "bead_id={BEAD_ID} case=toolchains_nonempty"
    );
}

#[test]
fn canonical_probes_nonempty() {
    let probes = canonical_probes(0x5678);
    assert!(
        !probes.is_empty(),
        "bead_id={BEAD_ID} case=probes_nonempty"
    );
}

#[test]
fn canonical_corpus_nonempty() {
    let corpus = build_canonical_corpus(0x9ABC);
    assert!(
        !corpus.is_empty(),
        "bead_id={BEAD_ID} case=corpus_nonempty"
    );
}

#[test]
fn coverage_has_subsystem_coverage() {
    let matrix = DeterminismMatrix::canonical(0xDEF0);
    let cov = compute_determinism_coverage(&matrix);

    assert!(
        !cov.by_subsystem.is_empty(),
        "bead_id={BEAD_ID} case=subsystem_coverage"
    );
    assert!(
        !cov.by_kind.is_empty(),
        "bead_id={BEAD_ID} case=kind_coverage"
    );
}
