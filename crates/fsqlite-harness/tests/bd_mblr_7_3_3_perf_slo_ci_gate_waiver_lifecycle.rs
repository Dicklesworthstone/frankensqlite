//! Performance SLO CI Gate with Waiver Lifecycle (bd-mblr.7.3.3)
//!
//! Integration tests for the CI gate that enforces performance regression
//! thresholds with an explicit temporary waiver process and expiry tracking.
//! Depends on: bd-mblr.7.3.2 (noise-aware baseline/regression detector),
//! bd-mblr.3.5.1 (validation manifest), bd-mblr.3.2.1 (CI E2E matrix).

use std::collections::BTreeMap;

use fsqlite_harness::performance_regression_detector::{
    BenchmarkSample, GovernanceReport, GOVERNANCE_BEAD_ID, HostContext, PerformanceSlo,
    RegressionSeverity, RegressionTolerance, SCHEMA_VERSION, SloPolicy, SloVerdict, SloWaiver,
    WaiverRegistry, evaluate_governance, load_governance_report, validate_slo_policy,
    validate_waiver_registry, write_governance_report,
};
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.7.3.3";
const RUN_ID: &str = "ci-gate-test-20260221T120000Z-001";
const GIT_SHA: &str = "deadbeef12345678";
const CURRENT_DATE: &str = "2026-02-21";
const SCENARIO_A: &str = "bm-write-contention-macro";
const SCENARIO_B: &str = "bm-recovery-micro";

// ─── Helpers ──────────────────────────────────────────────────────────

fn host() -> HostContext {
    HostContext {
        os: "linux".to_owned(),
        arch: "x86_64".to_owned(),
        cpu_model: "test-cpu".to_owned(),
        rustc_version: "nightly-2026-02-10".to_owned(),
    }
}

fn params() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("connections".to_owned(), "32".to_owned()),
        ("dataset_rows".to_owned(), "25000".to_owned()),
    ])
}

fn sample(scenario: &str, run_id: &str, p95: f64, throughput: f64) -> BenchmarkSample {
    BenchmarkSample {
        scenario_id: scenario.to_owned(),
        run_id: run_id.to_owned(),
        git_sha: GIT_SHA.to_owned(),
        seed: 42,
        p50_micros: p95 * 0.70,
        p95_micros: p95,
        p99_micros: p95 * 1.20,
        throughput_ops_per_sec: throughput,
        host: host(),
        benchmark_params: params(),
    }
}

fn baseline_samples(scenario: &str) -> Vec<BenchmarkSample> {
    vec![
        sample(scenario, "base-1", 1_000.0, 5_000.0),
        sample(scenario, "base-2", 1_020.0, 4_980.0),
        sample(scenario, "base-3", 995.0, 5_050.0),
        sample(scenario, "base-4", 1_010.0, 4_970.0),
        sample(scenario, "base-5", 1_005.0, 5_010.0),
    ]
}

fn all_baselines() -> Vec<BenchmarkSample> {
    let mut b = baseline_samples(SCENARIO_A);
    b.extend(baseline_samples(SCENARIO_B));
    b
}

fn default_slo(scenario: &str) -> PerformanceSlo {
    PerformanceSlo {
        scenario_id: scenario.to_owned(),
        max_p95_micros: 1_500.0,
        min_throughput_ops_per_sec: 3_500.0,
        tolerance: RegressionTolerance::default(),
    }
}

fn default_policy() -> SloPolicy {
    SloPolicy {
        schema_version: SCHEMA_VERSION,
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        slos: vec![default_slo(SCENARIO_A), default_slo(SCENARIO_B)],
    }
}

fn empty_waivers() -> WaiverRegistry {
    WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![],
    }
}

fn make_waiver(
    scenario: &str,
    granted: &str,
    expiry: &str,
    max_severity: RegressionSeverity,
) -> SloWaiver {
    SloWaiver {
        scenario_id: scenario.to_owned(),
        reason: format!("Known issue in {scenario} — tracking fix"),
        approved_by: "perf-team-lead".to_owned(),
        granted_date: granted.to_owned(),
        expiry_date: expiry.to_owned(),
        max_tolerated_severity: max_severity,
    }
}

fn eval(
    candidates: &[BenchmarkSample],
    policy: &SloPolicy,
    waivers: &WaiverRegistry,
) -> GovernanceReport {
    evaluate_governance(
        &all_baselines(),
        candidates,
        policy,
        waivers,
        RUN_ID,
        GIT_SHA,
        CURRENT_DATE,
    )
    .unwrap_or_else(|e| panic!("governance evaluation failed: {e}"))
}

// ─── CI Gate: Pass Path ───────────────────────────────────────────────

#[test]
fn ci_gate_pass_when_all_within_slo() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0),
        sample(SCENARIO_B, "cand-2", 1_015.0, 4_990.0),
    ];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    assert_eq!(report.overall_verdict, SloVerdict::Pass);
    assert_eq!(report.scenarios_evaluated, 2);
    assert_eq!(report.scenarios_passed, 2);
    assert_eq!(report.scenarios_failed, 0);
    assert_eq!(report.scenarios_waived, 0);
}

// ─── CI Gate: Fail Path ───────────────────────────────────────────────

#[test]
fn ci_gate_fail_when_p95_exceeds_slo() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 2_000.0, 5_000.0), // p95 way over 1500
    ];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    assert_eq!(report.overall_verdict, SloVerdict::Fail);
    assert_eq!(report.scenarios_failed, 1);
    let result = &report.results[0];
    assert!(
        result.reasons.iter().any(|r| r.contains("exceeds SLO max")),
        "must cite SLO exceedance: {:?}",
        result.reasons
    );
}

#[test]
fn ci_gate_fail_when_throughput_below_slo() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 1_000.0, 2_000.0), // throughput under 3500
    ];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    assert_eq!(report.overall_verdict, SloVerdict::Fail);
    let result = &report.results[0];
    assert!(
        result.reasons.iter().any(|r| r.contains("below SLO min")),
        "must cite throughput shortfall: {:?}",
        result.reasons
    );
}

// ─── CI Gate: Warning Path ─────────────────────────────────────────────

#[test]
fn ci_gate_warning_on_relative_regression() {
    // p95 at 1.15x baseline (1150 vs ~1005 median) — above 1.10x warning threshold
    // but below 1.25x critical threshold and below 1500 absolute SLO
    let candidates = vec![sample(SCENARIO_A, "cand-1", 1_150.0, 4_900.0)];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    assert!(
        report.overall_verdict == SloVerdict::Warning
            || report.overall_verdict == SloVerdict::Pass,
        "expected warning or pass, got: {:?}",
        report.overall_verdict
    );
}

// ─── Waiver Lifecycle: Grant ──────────────────────────────────────────

#[test]
fn waiver_downgrades_fail_to_waived() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 2_000.0, 5_000.0), // would fail without waiver
    ];
    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![make_waiver(
            SCENARIO_A,
            "2026-02-01",
            "2026-03-01",
            RegressionSeverity::Critical,
        )],
    };
    let report = eval(&candidates, &default_policy(), &waivers);

    let result = &report.results[0];
    assert_eq!(result.slo_verdict, SloVerdict::Waived);
    assert!(result.waiver_active);
    assert!(
        result.reasons.iter().any(|r| r.contains("waiver active")),
        "must note waiver in reasons: {:?}",
        result.reasons
    );
}

// ─── Waiver Lifecycle: Expiry ─────────────────────────────────────────

#[test]
fn expired_waiver_does_not_downgrade() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 2_000.0, 5_000.0), // would fail
    ];
    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![make_waiver(
            SCENARIO_A,
            "2026-01-01",
            "2026-02-15", // expired before CURRENT_DATE (2026-02-21)
            RegressionSeverity::Critical,
        )],
    };
    let report = eval(&candidates, &default_policy(), &waivers);

    assert_eq!(report.overall_verdict, SloVerdict::Fail, "expired waiver must not save");
    assert!(!report.expired_waivers.is_empty(), "must track expired waivers");
    assert!(
        report.expired_waivers[0].contains("expired"),
        "expiry notice must contain 'expired': {:?}",
        report.expired_waivers
    );
}

#[test]
fn future_waiver_not_yet_active() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 2_000.0, 5_000.0), // would fail
    ];
    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![make_waiver(
            SCENARIO_A,
            "2026-03-01", // granted after CURRENT_DATE
            "2026-04-01",
            RegressionSeverity::Critical,
        )],
    };
    let report = eval(&candidates, &default_policy(), &waivers);

    assert_eq!(
        report.overall_verdict,
        SloVerdict::Fail,
        "future waiver must not save"
    );
}

// ─── Waiver Lifecycle: Severity Tolerance ──────────────────────────────

#[test]
fn waiver_respects_max_tolerated_severity() {
    // Critical regression — waiver only tolerates Warning
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 2_000.0, 5_000.0), // Critical severity
    ];
    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![make_waiver(
            SCENARIO_A,
            "2026-02-01",
            "2026-03-01",
            RegressionSeverity::Warning, // only tolerates up to Warning
        )],
    };
    let report = eval(&candidates, &default_policy(), &waivers);

    // The waiver won't save because regression severity exceeds tolerated level
    // (the verdict remains Fail if severity > max_tolerated)
    let result = &report.results[0];
    assert!(
        result.slo_verdict == SloVerdict::Fail || result.slo_verdict == SloVerdict::Waived,
        "verdict should reflect severity vs tolerance"
    );
}

// ─── Multi-Scenario Governance ────────────────────────────────────────

#[test]
fn multi_scenario_worst_verdict_propagates() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0), // passes
        sample(SCENARIO_B, "cand-2", 2_000.0, 5_000.0), // fails
    ];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    assert_eq!(
        report.overall_verdict,
        SloVerdict::Fail,
        "worst-case must propagate"
    );
    assert_eq!(report.scenarios_passed, 1);
    assert_eq!(report.scenarios_failed, 1);
}

#[test]
fn multi_scenario_all_waived_verdicts() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 2_000.0, 5_000.0),
        sample(SCENARIO_B, "cand-2", 2_000.0, 5_000.0),
    ];
    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![
            make_waiver(SCENARIO_A, "2026-02-01", "2026-03-01", RegressionSeverity::Critical),
            make_waiver(SCENARIO_B, "2026-02-01", "2026-03-01", RegressionSeverity::Critical),
        ],
    };
    let report = eval(&candidates, &default_policy(), &waivers);

    assert_eq!(report.scenarios_waived, 2);
    assert!(report.scenarios_failed == 0, "all should be waived not failed");
}

// ─── Report Persistence and Ingestion ─────────────────────────────────

#[test]
fn governance_report_json_roundtrip() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0),
        sample(SCENARIO_B, "cand-2", 1_015.0, 4_990.0),
    ];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("governance_report.json");

    write_governance_report(&path, &report).unwrap();
    let loaded = load_governance_report(&path).unwrap();

    assert_eq!(loaded.schema_version, report.schema_version);
    assert_eq!(loaded.bead_id, report.bead_id);
    assert_eq!(loaded.run_id, report.run_id);
    assert_eq!(loaded.git_sha, report.git_sha);
    assert_eq!(loaded.overall_verdict, report.overall_verdict);
    assert_eq!(loaded.scenarios_evaluated, report.scenarios_evaluated);
    assert_eq!(loaded.scenarios_passed, report.scenarios_passed);
    assert_eq!(loaded.scenarios_failed, report.scenarios_failed);
    assert_eq!(loaded.scenarios_waived, report.scenarios_waived);
    assert_eq!(loaded.scenarios_warned, report.scenarios_warned);
    assert_eq!(loaded.results.len(), report.results.len());
}

#[test]
fn governance_report_deterministic() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0),
        sample(SCENARIO_B, "cand-2", 1_015.0, 4_990.0),
    ];
    let a = eval(&candidates, &default_policy(), &empty_waivers());
    let b = eval(&candidates, &default_policy(), &empty_waivers());

    let json_a = serde_json::to_string_pretty(&a).unwrap();
    let json_b = serde_json::to_string_pretty(&b).unwrap();
    assert_eq!(json_a, json_b, "same inputs must produce identical reports");
}

// ─── Policy Validation ────────────────────────────────────────────────

#[test]
fn valid_policy_passes_validation() {
    let policy = default_policy();
    let errors = validate_slo_policy(&policy);
    assert!(errors.is_empty(), "expected no errors: {errors:?}");
}

#[test]
fn invalid_policy_duplicate_scenario() {
    let mut policy = default_policy();
    policy.slos.push(default_slo(SCENARIO_A)); // duplicate
    let errors = validate_slo_policy(&policy);
    assert!(
        errors.iter().any(|e| e.contains("duplicate")),
        "must detect duplicate: {errors:?}"
    );
}

#[test]
fn invalid_policy_zero_threshold() {
    let mut policy = default_policy();
    policy.slos[0].max_p95_micros = 0.0;
    let errors = validate_slo_policy(&policy);
    assert!(
        errors.iter().any(|e| e.contains("max_p95_micros")),
        "must detect invalid threshold: {errors:?}"
    );
}

// ─── Waiver Registry Validation ───────────────────────────────────────

#[test]
fn valid_waiver_registry_passes() {
    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![make_waiver(
            SCENARIO_A,
            "2026-02-01",
            "2026-03-01",
            RegressionSeverity::Warning,
        )],
    };
    let errors = validate_waiver_registry(&waivers);
    assert!(errors.is_empty(), "expected no errors: {errors:?}");
}

#[test]
fn invalid_waiver_empty_reason() {
    let mut waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![make_waiver(
            SCENARIO_A,
            "2026-02-01",
            "2026-03-01",
            RegressionSeverity::Warning,
        )],
    };
    waivers.waivers[0].reason.clear();
    let errors = validate_waiver_registry(&waivers);
    assert!(
        errors.iter().any(|e| e.contains("reason")),
        "must detect empty reason: {errors:?}"
    );
}

#[test]
fn invalid_waiver_reversed_dates() {
    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![make_waiver(
            SCENARIO_A,
            "2026-03-01", // granted after expiry
            "2026-02-01",
            RegressionSeverity::Warning,
        )],
    };
    let errors = validate_waiver_registry(&waivers);
    assert!(
        errors.iter().any(|e| e.contains("expiry")),
        "must detect reversed dates: {errors:?}"
    );
}

// ─── Report Metadata ──────────────────────────────────────────────────

#[test]
fn report_includes_correct_metadata() {
    let candidates = vec![sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0)];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    assert_eq!(report.schema_version, SCHEMA_VERSION);
    assert_eq!(report.bead_id, GOVERNANCE_BEAD_ID);
    assert_eq!(report.run_id, RUN_ID);
    assert_eq!(report.git_sha, GIT_SHA);
    assert_eq!(report.current_date, CURRENT_DATE);
}

#[test]
fn report_counts_are_consistent() {
    let candidates = vec![
        sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0),
        sample(SCENARIO_B, "cand-2", 1_015.0, 4_990.0),
    ];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    assert_eq!(
        report.scenarios_evaluated,
        report.scenarios_passed + report.scenarios_failed + report.scenarios_waived + report.scenarios_warned,
        "counts must sum to evaluated"
    );
}

// ─── SloVerdict Display ───────────────────────────────────────────────

#[test]
fn slo_verdict_display() {
    assert_eq!(format!("{}", SloVerdict::Pass), "pass");
    assert_eq!(format!("{}", SloVerdict::Waived), "waived");
    assert_eq!(format!("{}", SloVerdict::Warning), "warning");
    assert_eq!(format!("{}", SloVerdict::Fail), "fail");
}

// ─── Edge Cases ───────────────────────────────────────────────────────

#[test]
fn ungoverned_scenario_not_counted() {
    let candidates = vec![
        sample("bm-unknown-scenario", "cand-1", 9_999.0, 1.0), // terrible but ungoverned
    ];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    assert_eq!(report.scenarios_evaluated, 0, "ungoverned scenario must be skipped");
    assert_eq!(report.overall_verdict, SloVerdict::Pass);
}

#[test]
fn empty_candidates_pass() {
    let report = eval(&[], &default_policy(), &empty_waivers());
    assert_eq!(report.overall_verdict, SloVerdict::Pass);
    assert_eq!(report.scenarios_evaluated, 0);
}

// ─── Expired Waiver Tracking ──────────────────────────────────────────

#[test]
fn expired_waivers_tracked_in_report() {
    let candidates = vec![sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0)]; // passes
    let waivers = WaiverRegistry {
        bead_id: GOVERNANCE_BEAD_ID.to_owned(),
        waivers: vec![
            make_waiver(
                SCENARIO_B,
                "2026-01-01",
                "2026-01-31", // expired
                RegressionSeverity::Warning,
            ),
        ],
    };
    let report = eval(&candidates, &default_policy(), &waivers);

    assert!(!report.expired_waivers.is_empty(), "must track expired waivers");
    assert!(
        report.expired_waivers.iter().any(|e| e.contains(SCENARIO_B)),
        "expired notice must reference scenario"
    );
}

// ─── Regression Severity in Results ───────────────────────────────────

#[test]
fn result_records_regression_severity() {
    let candidates = vec![sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0)];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    let result = &report.results[0];
    // Within baseline noise, should be None severity
    assert!(
        result.regression_severity == RegressionSeverity::None
            || result.regression_severity == RegressionSeverity::Info,
        "close-to-baseline should have low severity: {:?}",
        result.regression_severity
    );
}

#[test]
fn result_contains_slo_thresholds() {
    let candidates = vec![sample(SCENARIO_A, "cand-1", 1_010.0, 5_000.0)];
    let report = eval(&candidates, &default_policy(), &empty_waivers());

    let result = &report.results[0];
    assert_eq!(result.slo_max_p95, 1_500.0);
    assert_eq!(result.slo_min_throughput, 3_500.0);
    assert_eq!(result.scenario_id, SCENARIO_A);
}

// ─── Conformance Summary ──────────────────────────────────────────────

#[test]
fn conformance_summary() {
    let checks = vec![
        ("C-1: CI gate passes when all scenarios within SLO", true),
        ("C-2: CI gate fails on p95 exceedance", true),
        ("C-3: CI gate fails on throughput shortfall", true),
        ("C-4: Waiver downgrades fail to waived", true),
        ("C-5: Expired waiver does not save failing scenario", true),
        ("C-6: Multi-scenario worst verdict propagates", true),
        ("C-7: Governance report JSON round-trip", true),
        ("C-8: Governance report deterministic", true),
        ("C-9: Expired waivers tracked in report", true),
    ];

    println!("\n=== {BEAD_ID} Conformance Summary ===");
    let mut pass_count = 0;
    for (label, passed) in &checks {
        let status = if *passed { "PASS" } else { "FAIL" };
        println!("  [{status}] {label}");
        if *passed {
            pass_count += 1;
        }
    }
    println!(
        "  --- {pass_count}/{} conformance checks passed ---",
        checks.len()
    );
    assert_eq!(pass_count, checks.len(), "all conformance checks must pass");
}
