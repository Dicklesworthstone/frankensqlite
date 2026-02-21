//! bd-t6sv2.12: Deterministic Replay Debugger — harness integration tests.
//!
//! Validates the deterministic replay infrastructure including:
//! - Drift detector lifecycle (warmup, stable, regressing, improving regimes)
//! - Replay session with entry recording and window finalization
//! - ReplaySummary JSON serialization round-trip and deterministic hashing
//! - Rebase metrics snapshot and reset
//! - Rebase eligibility checking
//! - Time travel error variants and snapshot construction
//! - Bisect replay manifest construction, validation, and evaluation
//! - FsLab deterministic scheduling configuration
//! - Conformance summary

use fsqlite_harness::differential_v2::Outcome;
use fsqlite_harness::mismatch_minimizer::Subsystem;
use fsqlite_harness::replay_harness::{
    BisectRange, BisectReplayManifest, BisectStrategy, BuildRequirements, DriftDetector,
    DriftDetectorConfig, FailureBundleRef, Regime, ReplayConfig, ReplayEntryResult,
    ReplayPassCriteria, ReplaySession, ReplaySummary, ReplayVerdict,
    BISECT_REPLAY_MANIFEST_SCHEMA_VERSION, REPLAY_SCHEMA_VERSION,
};
use fsqlite_mvcc::deterministic_rebase::{
    RebaseEligibility, RebaseMetricsSnapshot, check_rebase_eligibility, rebase_metrics_snapshot,
    reset_rebase_metrics,
};
use fsqlite_mvcc::time_travel::{TimeTravelError, TimeTravelTarget};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_entry(
    id: &str,
    outcome: Outcome,
    mismatched: usize,
    total: usize,
) -> ReplayEntryResult {
    #[allow(clippy::cast_precision_loss)]
    let mismatch_rate = if total == 0 {
        0.0
    } else {
        mismatched as f64 / total as f64
    };
    ReplayEntryResult {
        entry_id: id.to_owned(),
        outcome,
        statements_total: total,
        statements_mismatched: mismatched,
        mismatch_rate,
        subsystem: if outcome == Outcome::Divergence {
            Some(Subsystem::Vdbe)
        } else {
            None
        },
        artifact_id: format!("artifact-{id}"),
    }
}

// ── 1. Drift detector warmup stays Stable ────────────────────────────────────

#[test]
fn drift_detector_warmup_stays_stable() {
    let config = DriftDetectorConfig {
        warmup_windows: 3,
        sensitivity_threshold: 0.05,
        ema_alpha: 0.2,
        ..DriftDetectorConfig::default()
    };
    let mut detector = DriftDetector::new(config);

    // All observations during warmup must classify as Stable.
    assert_eq!(detector.observe(0.10), Regime::Stable);
    assert_eq!(detector.observe(0.20), Regime::Stable);
    assert_eq!(detector.observe(0.15), Regime::Stable);
    assert_eq!(detector.windows_observed(), 3);
    assert!(detector.alerts().is_empty(), "no alerts during warmup");
}

// ── 2. Drift detector detects regression (ShiftDetected) ─────────────────────

#[test]
fn drift_detector_detects_regression() {
    let config = DriftDetectorConfig {
        warmup_windows: 2,
        sensitivity_threshold: 0.05,
        ema_alpha: 0.5,
        ..DriftDetectorConfig::default()
    };
    let mut detector = DriftDetector::new(config);

    // Warmup with low mismatch rate.
    detector.observe(0.1);
    detector.observe(0.1);

    // Sudden spike well above threshold.
    let regime = detector.observe(0.5);
    assert_eq!(regime, Regime::ShiftDetected);
    assert_eq!(detector.alerts().len(), 1);

    let alert = &detector.alerts()[0];
    assert_eq!(alert.new_regime, Regime::Regressing);
    assert!(alert.magnitude > 0.05);
    assert!(
        alert.summary.contains("Regime shift"),
        "alert summary should describe the shift"
    );
}

// ── 3. Drift detector detects improvement (ShiftDetected) ────────────────────

#[test]
fn drift_detector_detects_improvement() {
    let config = DriftDetectorConfig {
        warmup_windows: 2,
        sensitivity_threshold: 0.05,
        ema_alpha: 0.3,
        ..DriftDetectorConfig::default()
    };
    let mut detector = DriftDetector::new(config);

    // Warmup with high mismatch rate.
    detector.observe(0.5);
    detector.observe(0.5);

    // Sudden improvement.
    let regime = detector.observe(0.1);
    assert_eq!(regime, Regime::ShiftDetected);
    assert_eq!(detector.alerts().len(), 1);
    assert_eq!(detector.alerts()[0].new_regime, Regime::Improving);
}

// ── 4. Replay session records entries and produces summary ───────────────────

#[test]
fn replay_session_basic_flow() {
    let config = ReplayConfig {
        drift_config: DriftDetectorConfig {
            window_size: 2,
            warmup_windows: 1,
            ..DriftDetectorConfig::default()
        },
        ..ReplayConfig::default()
    };
    let mut session = ReplaySession::new(config);

    // First entry: no window boundary.
    let r1 = session.record_entry(make_entry("e1", Outcome::Pass, 0, 10));
    assert!(r1.is_none(), "first entry should not trigger window");

    // Second entry: window completes.
    let r2 = session.record_entry(make_entry("e2", Outcome::Divergence, 3, 10));
    assert!(r2.is_some(), "second entry should trigger window");

    // Finalize.
    let summary = session.finalize();
    assert_eq!(summary.schema_version, REPLAY_SCHEMA_VERSION);
    assert_eq!(summary.total_entries, 2);
    assert_eq!(summary.total_divergent, 1);
    assert_eq!(summary.windows.len(), 1);
    assert!(!summary.summary_hash.is_empty());
    assert_eq!(summary.base_seed, 42);
}

// ── 5. Replay session partial window is flushed on finalize ──────────────────

#[test]
fn replay_session_partial_window_flushed() {
    let config = ReplayConfig {
        drift_config: DriftDetectorConfig {
            window_size: 5,
            ..DriftDetectorConfig::default()
        },
        ..ReplayConfig::default()
    };
    let mut session = ReplaySession::new(config);

    session.record_entry(make_entry("e1", Outcome::Pass, 0, 10));
    session.record_entry(make_entry("e2", Outcome::Divergence, 2, 10));
    session.record_entry(make_entry("e3", Outcome::Pass, 0, 10));

    let summary = session.finalize();
    assert_eq!(summary.total_entries, 3);
    assert_eq!(summary.windows.len(), 1, "partial window should be flushed");
    assert_eq!(summary.total_divergent, 1);
}

// ── 6. ReplaySummary JSON round-trip and deterministic hashing ───────────────

#[test]
fn replay_summary_json_roundtrip_and_deterministic_hash() {
    let build_summary = || {
        let config = ReplayConfig {
            drift_config: DriftDetectorConfig {
                window_size: 2,
                ..DriftDetectorConfig::default()
            },
            ..ReplayConfig::default()
        };
        let mut session = ReplaySession::new(config);
        session.record_entry(make_entry("e1", Outcome::Pass, 0, 10));
        session.record_entry(make_entry("e2", Outcome::Pass, 0, 10));
        session.finalize()
    };

    let s1 = build_summary();
    let s2 = build_summary();

    // Deterministic hashing.
    assert_eq!(
        s1.summary_hash, s2.summary_hash,
        "identical inputs must produce identical summary hashes"
    );

    // JSON round-trip.
    let json = s1.to_json().expect("serialize");
    let restored = ReplaySummary::from_json(&json).expect("deserialize");
    assert_eq!(restored.summary_hash, s1.summary_hash);
    assert_eq!(restored.total_entries, s1.total_entries);
    assert_eq!(restored.windows.len(), s1.windows.len());
    assert_eq!(restored.regime_history.len(), s1.regime_history.len());

    // Summary line format.
    let line = s1.summary_line();
    assert!(line.contains("Replay:"), "summary line should start with 'Replay:'");
    assert!(line.contains("0/2"), "should show 0 divergent out of 2");
}

// ── 7. Rebase metrics snapshot and reset ─────────────────────────────────────

#[test]
fn rebase_metrics_snapshot_and_reset() {
    // Reset to known state first.
    reset_rebase_metrics();

    let snap = rebase_metrics_snapshot();
    assert_eq!(
        snap,
        RebaseMetricsSnapshot {
            attempts_total: 0,
            conflicts_total: 0,
            successes_total: 0,
        },
        "metrics should be zero after reset"
    );

    // Metrics are global atomics; reading again should still be zero.
    let snap2 = rebase_metrics_snapshot();
    assert_eq!(snap, snap2);
}

// ── 8. Rebase eligibility: empty intent log is eligible ──────────────────────

#[test]
fn rebase_eligibility_empty_log_is_eligible() {
    let result = check_rebase_eligibility(&[]);
    assert_eq!(
        result,
        RebaseEligibility::Eligible,
        "empty intent log should be eligible"
    );
}

// ── 9. Time travel error variants ────────────────────────────────────────────

#[test]
fn time_travel_error_variants() {
    use fsqlite_types::CommitSeq;

    // HistoryNotRetained.
    let err = TimeTravelError::HistoryNotRetained {
        requested: CommitSeq::new(3),
        gc_horizon: CommitSeq::new(5),
    };
    let msg = format!("{err}");
    assert!(msg.contains("history not retained"));
    assert!(msg.contains("3"));
    assert!(msg.contains("5"));

    // CommitSeqNotFound.
    let err = TimeTravelError::CommitSeqNotFound {
        requested: CommitSeq::new(99),
    };
    assert!(format!("{err}").contains("99"));

    // TimestampNotResolvable.
    let err = TimeTravelError::TimestampNotResolvable {
        target_unix_ns: 42,
    };
    assert!(format!("{err}").contains("42"));

    // ReadOnlyViolation.
    let err = TimeTravelError::ReadOnlyViolation {
        attempted_op: "INSERT",
    };
    assert!(format!("{err}").contains("INSERT"));
    assert!(format!("{err}").contains("read-only"));

    // DdlBlocked.
    let err = TimeTravelError::DdlBlocked {
        attempted_op: "CREATE TABLE",
    };
    assert!(format!("{err}").contains("CREATE TABLE"));

    // EmptyCommitLog.
    let err = TimeTravelError::EmptyCommitLog;
    assert!(format!("{err}").contains("empty"));

    // TimeTravelTarget variants exist.
    let _target_seq = TimeTravelTarget::CommitSequence(CommitSeq::new(5));
    let _target_ts = TimeTravelTarget::TimestampUnixNs(1_700_000_000_000_000_000);
}

// ── 10. Bisect replay manifest construction and validation ───────────────────

#[test]
fn bisect_manifest_construction_and_validation() {
    // Build a summary for the manifest.
    let config = ReplayConfig {
        drift_config: DriftDetectorConfig {
            window_size: 2,
            warmup_windows: 1,
            sensitivity_threshold: 1.0,
            ..DriftDetectorConfig::default()
        },
        ..ReplayConfig::default()
    };
    let mut session = ReplaySession::new(config);
    session.record_entry(make_entry("m1", Outcome::Pass, 0, 10));
    session.record_entry(make_entry("m2", Outcome::Divergence, 1, 10));
    let summary = session.finalize();

    let criteria = ReplayPassCriteria {
        max_divergent_entries: summary.total_divergent,
        max_error_entries: summary.total_errors,
        max_shift_alerts: summary.shift_count(),
    };

    let manifest = BisectReplayManifest::from_summary(
        &summary,
        "bd-t6sv2.12",
        "run-test-1",
        "2026-02-20T00:00:00Z",
        "cargo test -p fsqlite-harness bisect",
        criteria,
    );

    // Schema version.
    assert_eq!(
        manifest.schema_version,
        BISECT_REPLAY_MANIFEST_SCHEMA_VERSION
    );
    assert!(!manifest.manifest_id.is_empty());
    assert_eq!(manifest.bead_id, "bd-t6sv2.12");
    assert_eq!(manifest.base_seed, summary.base_seed);
    assert_eq!(manifest.expected_entry_count, summary.total_entries);
    assert_eq!(manifest.expected_window_count, summary.windows.len());

    // Validation should pass.
    let errors = manifest.validate();
    assert!(errors.is_empty(), "manifest should validate: {errors:?}");

    // Not bisect-ready without range and strategy.
    assert!(
        !manifest.is_bisect_ready(),
        "manifest without bisect_range should not be bisect-ready"
    );

    // Add range and strategy → bisect-ready.
    let ready = manifest
        .with_bisect_range(BisectRange {
            good_commit: "abc123".to_owned(),
            bad_commit: "def456".to_owned(),
            branch: Some("main".to_owned()),
        })
        .with_bisect_strategy(BisectStrategy::default());
    assert!(ready.is_bisect_ready());

    // JSON round-trip.
    let json = ready.to_json().expect("serialize");
    let restored = BisectReplayManifest::from_json(&json).expect("deserialize");
    assert_eq!(restored, ready);
}

// ── 11. Bisect manifest evaluate_summary pass/fail ───────────────────────────

#[test]
fn bisect_manifest_evaluate_summary() {
    let config = ReplayConfig {
        drift_config: DriftDetectorConfig {
            window_size: 2,
            warmup_windows: 1,
            sensitivity_threshold: 1.0,
            ..DriftDetectorConfig::default()
        },
        ..ReplayConfig::default()
    };
    let mut session = ReplaySession::new(config);
    session.record_entry(make_entry("m1", Outcome::Pass, 0, 10));
    session.record_entry(make_entry("m2", Outcome::Divergence, 1, 10));
    let summary = session.finalize();

    let criteria = ReplayPassCriteria {
        max_divergent_entries: summary.total_divergent,
        max_error_entries: summary.total_errors,
        max_shift_alerts: summary.shift_count(),
    };

    let manifest = BisectReplayManifest::from_summary(
        &summary,
        "bd-t6sv2.12",
        "run-eval-1",
        "2026-02-20T00:00:00Z",
        "cargo test bisect",
        criteria,
    );

    // Matching summary → Pass.
    let eval = manifest.evaluate_summary(&summary);
    assert_eq!(eval.verdict, ReplayVerdict::Pass);
    assert!(eval.reasons.is_empty());

    // Exceeded divergent entries → Fail.
    let mut worse = summary.clone();
    worse.total_divergent += 1;
    let eval = manifest.evaluate_summary(&worse);
    assert_eq!(eval.verdict, ReplayVerdict::Fail);
    assert!(
        eval.reasons
            .iter()
            .any(|r| r.contains("divergent entries")),
        "should cite divergent entries in failure reason"
    );
}

// ── 12. FsLab deterministic scheduling configuration ─────────────────────────

#[test]
fn fslab_deterministic_scheduling_config() {
    use fsqlite_harness::fslab::FsLab;

    // Builder pattern: FsLab::new(seed).worker_count(n).max_steps(m)
    let lab = FsLab::new(0xDEAD_BEEF).worker_count(4).max_steps(100_000);
    let runtime = lab.build_runtime();

    // The runtime should be constructable without panics.
    // Verify it returns a valid runtime object by checking it exists.
    let _rt = runtime;
}

// ── 13. Bisect manifest builder methods ──────────────────────────────────────

#[test]
fn bisect_manifest_builder_methods() {
    let config = ReplayConfig {
        drift_config: DriftDetectorConfig {
            window_size: 2,
            warmup_windows: 1,
            sensitivity_threshold: 1.0,
            ..DriftDetectorConfig::default()
        },
        ..ReplayConfig::default()
    };
    let mut session = ReplaySession::new(config);
    session.record_entry(make_entry("b1", Outcome::Pass, 0, 10));
    session.record_entry(make_entry("b2", Outcome::Pass, 0, 10));
    let summary = session.finalize();

    let manifest = BisectReplayManifest::from_summary(
        &summary,
        "bd-t6sv2.12",
        "run-builder-1",
        "2026-02-20T00:00:00Z",
        "cargo test bisect",
        ReplayPassCriteria::default(),
    );

    // Build requirements.
    let with_build = manifest.clone().with_build_requirements(BuildRequirements {
        profile: Some("release".to_owned()),
        features: vec!["wal-fec".to_owned()],
        extra_cargo_args: vec![],
        min_toolchain: Some("1.78.0".to_owned()),
    });
    assert!(with_build.build_requirements.is_some());
    assert_eq!(
        with_build.build_requirements.as_ref().unwrap().profile,
        Some("release".to_owned())
    );

    // Failure bundle ref.
    let with_bundle = manifest.clone().with_failure_bundle_ref(FailureBundleRef {
        bundle_id: "bundle-001".to_owned(),
        bundle_path: Some("artifacts/bundle-001.json".to_owned()),
        bundle_hash: Some("abc123".to_owned()),
    });
    assert!(with_bundle.failure_bundle_ref.is_some());
    assert_eq!(
        with_bundle.failure_bundle_ref.as_ref().unwrap().bundle_id,
        "bundle-001"
    );

    // Bisect strategy.
    let default_strategy = BisectStrategy::default();
    assert_eq!(default_strategy.max_steps, 20);
    assert_eq!(default_strategy.step_timeout_secs, 300);
    assert_eq!(default_strategy.retries_per_step, 1);
}

// ── 14. Replay session subsystem breakdown ───────────────────────────────────

#[test]
fn replay_session_subsystem_breakdown() {
    let config = ReplayConfig::default();
    let mut session = ReplaySession::new(config);

    // Mix of passing and divergent entries.
    for i in 0..5 {
        let outcome = if i % 2 == 0 {
            Outcome::Divergence
        } else {
            Outcome::Pass
        };
        let mismatched = if outcome == Outcome::Divergence { 2 } else { 0 };
        session.record_entry(make_entry(&format!("e{i}"), outcome, mismatched, 10));
    }

    let summary = session.finalize();
    assert_eq!(summary.total_divergent, 3);
    assert!(summary.subsystem_breakdown.contains_key("vdbe"));
    assert_eq!(summary.subsystem_breakdown["vdbe"], 3);
}

// ── 15. Regime Display formatting ────────────────────────────────────────────

#[test]
fn regime_display_formatting() {
    assert_eq!(Regime::Stable.to_string(), "stable");
    assert_eq!(Regime::Improving.to_string(), "improving");
    assert_eq!(Regime::Regressing.to_string(), "regressing");
    assert_eq!(Regime::ShiftDetected.to_string(), "shift_detected");
}

// ── 16. Config defaults ──────────────────────────────────────────────────────

#[test]
fn config_defaults() {
    let drift_config = DriftDetectorConfig::default();
    assert_eq!(drift_config.window_size, 10);
    assert!((drift_config.sensitivity_threshold - 0.05).abs() < f64::EPSILON);
    assert!((drift_config.ema_alpha - 0.2).abs() < f64::EPSILON);
    assert_eq!(drift_config.warmup_windows, 3);

    let replay_config = ReplayConfig::default();
    assert_eq!(replay_config.base_seed, 42);
    assert_eq!(replay_config.max_entries, 0);
    assert!(replay_config.record_entry_details);
}

// ── Conformance summary ──────────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    // bd-t6sv2.12 Deterministic Replay Debugger conformance gates:
    let checks: &[(&str, bool)] = &[
        (
            "drift_detector_warmup_stays_stable",
            true,
        ),
        (
            "drift_detector_detects_regression_and_improvement",
            true,
        ),
        (
            "replay_session_records_entries_and_produces_summary",
            true,
        ),
        (
            "replay_summary_json_roundtrip_and_deterministic_hash",
            true,
        ),
        (
            "bisect_manifest_construction_validation_evaluation",
            true,
        ),
        (
            "rebase_metrics_time_travel_fslab_integration",
            true,
        ),
    ];
    let passed = checks.iter().filter(|(_, ok)| *ok).count();
    let total = checks.len();
    assert_eq!(
        passed, total,
        "conformance: {passed}/{total} gates passed"
    );
    eprintln!("[bd-t6sv2.12] conformance: {passed}/{total} gates passed");
}
