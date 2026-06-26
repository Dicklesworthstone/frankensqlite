//! Integration tests for bd-1dp9.4.1 — WAL/checkpoint/journal-mode parity closure wave.
//!
//! Validates the parity assessment engine that aggregates evidence from
//! journal-mode, checkpoint-mode, sentinel, and transition tests.

use fsqlite_harness::wal_journal_parity::{
    CheckpointMode, JournalMode, ParityVerdict, WAL_JOURNAL_PARITY_BEAD_ID,
    WAL_JOURNAL_SCHEMA_VERSION, WalJournalParityConfig, WalJournalParityReport,
    assess_wal_journal_parity, load_wal_journal_report, write_wal_journal_report,
};

const BEAD_ID: &str = "bd-1dp9.4.1";

// ---------------------------------------------------------------------------
// Full assessment
// ---------------------------------------------------------------------------

#[test]
fn assessment_produces_parity_verdict() {
    let cfg = WalJournalParityConfig::default();
    let report = assess_wal_journal_parity(&cfg);

    assert_eq!(
        report.verdict,
        ParityVerdict::Parity,
        "bead_id={BEAD_ID} case=verdict_parity"
    );
    assert_eq!(
        report.bead_id, WAL_JOURNAL_PARITY_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        report.schema_version, WAL_JOURNAL_SCHEMA_VERSION,
        "bead_id={BEAD_ID} case=schema_version"
    );
}

#[test]
fn all_six_journal_modes_tested() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    assert_eq!(
        report.journal_modes_tested.len(),
        6,
        "bead_id={BEAD_ID} case=journal_count"
    );
    assert_eq!(
        report.journal_modes_at_parity.len(),
        6,
        "bead_id={BEAD_ID} case=journal_parity_count"
    );

    for mode in JournalMode::ALL {
        assert!(
            report
                .journal_modes_tested
                .contains(&mode.as_str().to_owned()),
            "bead_id={BEAD_ID} case=journal_mode_present mode={}",
            mode,
        );
    }
}

#[test]
fn all_four_checkpoint_modes_tested() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    assert_eq!(
        report.checkpoint_modes_tested.len(),
        4,
        "bead_id={BEAD_ID} case=checkpoint_count"
    );
    assert_eq!(
        report.checkpoint_modes_at_parity.len(),
        4,
        "bead_id={BEAD_ID} case=checkpoint_parity_count"
    );

    for mode in CheckpointMode::ALL {
        assert!(
            report
                .checkpoint_modes_tested
                .contains(&mode.as_str().to_owned()),
            "bead_id={BEAD_ID} case=checkpoint_mode_present mode={}",
            mode,
        );
    }
}

#[test]
fn sentinel_and_transition_parity() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    assert!(
        report.non_wal_sentinel_parity,
        "bead_id={BEAD_ID} case=sentinel_parity"
    );
    assert!(
        report.mode_transition_parity,
        "bead_id={BEAD_ID} case=transition_parity"
    );
    assert!(
        report.data_integrity_verified,
        "bead_id={BEAD_ID} case=data_integrity"
    );
}

#[test]
#[allow(clippy::float_cmp)]
fn parity_score_bounds() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    assert!(
        report.parity_score >= 0.0 && report.parity_score <= 1.0,
        "bead_id={BEAD_ID} case=score_bounds score={}",
        report.parity_score,
    );
    assert_eq!(
        report.parity_score, 1.0,
        "bead_id={BEAD_ID} case=full_parity_score"
    );
    assert_eq!(
        report.checks_at_parity, report.total_checks,
        "bead_id={BEAD_ID} case=all_checks_pass"
    );
}

#[test]
fn no_known_gaps_after_parity_closure() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    assert!(
        report.known_gaps.is_empty(),
        "bead_id={BEAD_ID} case=no_known_gaps_after_parity gaps={:?}",
        report.known_gaps,
    );
}

// ---------------------------------------------------------------------------
// Individual checks
// ---------------------------------------------------------------------------

#[test]
fn check_entries_cover_all_categories() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    let categories: Vec<&str> = report.checks.iter().map(|c| c.category.as_str()).collect();
    assert!(
        categories.contains(&"journal_mode"),
        "bead_id={BEAD_ID} case=has_journal_checks"
    );
    assert!(
        categories.contains(&"checkpoint"),
        "bead_id={BEAD_ID} case=has_checkpoint_checks"
    );
    assert!(
        categories.contains(&"sentinel"),
        "bead_id={BEAD_ID} case=has_sentinel_check"
    );
    assert!(
        categories.contains(&"transition"),
        "bead_id={BEAD_ID} case=has_transition_checks"
    );
    assert!(
        categories.contains(&"integrity"),
        "bead_id={BEAD_ID} case=has_integrity_check"
    );
}

#[test]
fn all_checks_have_details() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    for check in &report.checks {
        assert!(
            !check.detail.is_empty(),
            "bead_id={BEAD_ID} case=check_detail name={}",
            check.check_name,
        );
        assert!(
            check.parity_achieved,
            "bead_id={BEAD_ID} case=check_parity name={}",
            check.check_name,
        );
    }
}

// ---------------------------------------------------------------------------
// Triage and summary
// ---------------------------------------------------------------------------

#[test]
fn triage_line_has_all_fields() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());
    let line = report.triage_line();

    for field in [
        "verdict=",
        "parity=",
        "journal=",
        "ckpt=",
        "sentinel=",
        "transitions=",
        "gaps=",
    ] {
        assert!(
            line.contains(field),
            "bead_id={BEAD_ID} case=triage_field field={field}"
        );
    }
}

#[test]
fn summary_is_informative() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    assert!(
        !report.summary.is_empty(),
        "bead_id={BEAD_ID} case=summary_nonempty"
    );
    assert!(
        report.summary.contains("PARITY"),
        "bead_id={BEAD_ID} case=summary_verdict"
    );
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::float_cmp)]
fn report_json_roundtrip() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    let json = report.to_json().expect("serialize");
    let parsed = WalJournalParityReport::from_json(&json).expect("parse");

    assert_eq!(parsed.verdict, report.verdict);
    assert_eq!(parsed.parity_score, report.parity_score);
    assert_eq!(parsed.total_checks, report.total_checks);
    assert_eq!(
        parsed.journal_modes_tested.len(),
        report.journal_modes_tested.len()
    );
    assert_eq!(parsed.known_gaps.len(), report.known_gaps.len());
}

#[test]
fn report_file_roundtrip() {
    let report = assess_wal_journal_parity(&WalJournalParityConfig::default());

    let dir = std::env::temp_dir().join("fsqlite-wal-journal-integ");
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("parity-integ-test.json");

    write_wal_journal_report(&path, &report).expect("write");
    let loaded = load_wal_journal_report(&path).expect("load");

    assert_eq!(loaded.verdict, report.verdict);
    assert_eq!(loaded.checks_at_parity, report.checks_at_parity);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::float_cmp)]
fn assessment_is_deterministic() {
    let cfg = WalJournalParityConfig::default();
    let r1 = assess_wal_journal_parity(&cfg);
    let r2 = assess_wal_journal_parity(&cfg);

    assert_eq!(r1.verdict, r2.verdict, "bead_id={BEAD_ID} case=det_verdict");
    assert_eq!(
        r1.parity_score, r2.parity_score,
        "bead_id={BEAD_ID} case=det_score"
    );
    assert_eq!(
        r1.to_json().unwrap(),
        r2.to_json().unwrap(),
        "bead_id={BEAD_ID} case=det_json"
    );
}
