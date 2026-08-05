//! Integration tests for bd-1dp9.4.3 — File-format maintenance command parity.

use fsqlite_harness::maintenance_parity::{
    MAINTENANCE_PARITY_BEAD_ID, MAINTENANCE_SCHEMA_VERSION, MaintenanceCommand,
    MaintenanceParityConfig, MaintenanceParityReport, MaintenanceVerdict,
    assess_maintenance_parity, load_maintenance_report, write_maintenance_report,
};

const BEAD_ID: &str = "bd-1dp9.4.3";

#[test]
fn assessment_records_partial_vacuum_parity() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    assert_eq!(
        report.verdict,
        MaintenanceVerdict::Partial,
        "bead_id={BEAD_ID} case=verdict"
    );
    assert_eq!(
        report.bead_id, MAINTENANCE_PARITY_BEAD_ID,
        "bead_id={BEAD_ID} case=bead_id"
    );
    assert_eq!(
        report.schema_version, MAINTENANCE_SCHEMA_VERSION,
        "bead_id={BEAD_ID} case=schema"
    );
}

#[test]
fn all_six_commands_tested() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    assert_eq!(
        report.commands_tested.len(),
        6,
        "bead_id={BEAD_ID} case=cmd_count"
    );
    for cmd in MaintenanceCommand::ALL {
        assert!(
            report.commands_tested.contains(&cmd.as_str().to_owned()),
            "bead_id={BEAD_ID} case=cmd_present cmd={cmd}",
        );
    }
}

#[test]
fn five_commands_are_at_parity_and_vacuum_is_excluded() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    assert_eq!(
        report.commands_at_parity.len(),
        5,
        "bead_id={BEAD_ID} case=cmd_parity"
    );
    assert!(
        !report.commands_at_parity.iter().any(|cmd| cmd == "vacuum"),
        "bead_id={BEAD_ID} case=vacuum_not_at_parity"
    );
}

#[test]
fn integrity_check_ok() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    assert!(
        report.integrity_check_ok,
        "bead_id={BEAD_ID} case=integrity"
    );
}

#[test]
fn parity_score_records_vacuum_limit() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    assert!(
        (report.parity_score - 0.833_333).abs() < f64::EPSILON,
        "bead_id={BEAD_ID} case=score"
    );
    assert_eq!(
        report.checks_at_parity, 10,
        "bead_id={BEAD_ID} case=checks_at_parity"
    );
    assert_eq!(
        report.total_checks, 12,
        "bead_id={BEAD_ID} case=total_checks"
    );
    assert_eq!(
        report
            .checks
            .iter()
            .filter(|check| check.command == "vacuum" && !check.parity_achieved)
            .count(),
        2,
        "bead_id={BEAD_ID} case=vacuum_partial_checks"
    );
}

#[test]
fn triage_line_has_fields() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    let line = report.triage_line();
    for field in ["verdict=", "parity=", "commands=", "integrity="] {
        assert!(
            line.contains(field),
            "bead_id={BEAD_ID} case=triage field={field}"
        );
    }
}

#[test]
fn summary_is_informative() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    assert!(!report.summary.is_empty(), "bead_id={BEAD_ID} case=summary");
    assert!(
        report.summary.contains("PARTIAL"),
        "bead_id={BEAD_ID} case=summary_verdict"
    );
}

#[test]
fn report_json_roundtrip() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    let json = report.to_json().expect("serialize");
    let parsed = MaintenanceParityReport::from_json(&json).expect("parse");
    assert_eq!(parsed.verdict, report.verdict);
    assert!(
        (parsed.parity_score - report.parity_score).abs() < f64::EPSILON,
        "bead_id={BEAD_ID} case=roundtrip_score"
    );
}

#[test]
fn report_file_roundtrip() {
    let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
    let dir = std::env::temp_dir().join("fsqlite-maint-integ");
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("maint-integ.json");
    write_maintenance_report(&path, &report).expect("write");
    let loaded = load_maintenance_report(&path).expect("load");
    assert_eq!(loaded.verdict, report.verdict);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);
}

#[test]
fn assessment_is_deterministic() {
    let cfg = MaintenanceParityConfig::default();
    let r1 = assess_maintenance_parity(&cfg);
    let r2 = assess_maintenance_parity(&cfg);
    assert_eq!(
        r1.to_json().unwrap(),
        r2.to_json().unwrap(),
        "bead_id={BEAD_ID} case=det"
    );
}
