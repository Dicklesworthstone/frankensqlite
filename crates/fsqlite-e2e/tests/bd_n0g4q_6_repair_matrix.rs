//! Bead bd-n0g4q.6: E2E corruption/repair matrix with evidence validation.
//!
//! This suite exercises seven deterministic corruption classes using the
//! recovery runner, then validates:
//! - expected recovery classification per scenario,
//! - presence/quality of structured repair evidence,
//! - non-zero BLAKE3 witness + chain hashes in the evidence ledger.

use fsqlite_e2e::corruption_scenarios::{scenario_catalog, CorruptionScenario};
use fsqlite_e2e::recovery_runner::{run_recovery, RecoveryClassification};
use fsqlite_wal::{raptorq_repair_evidence_snapshot, reset_raptorq_repair_telemetry};

fn scenario_by_name(name: &str) -> CorruptionScenario {
    scenario_catalog()
        .into_iter()
        .find(|scenario| scenario.name == name)
        .unwrap_or_else(|| panic!("missing corruption scenario: {name}"))
}

#[derive(Debug, Clone, Copy)]
enum ExpectedClass {
    Recovered,
    LostInsufficientOrMissingSymbols,
    LostRecoveryDisabled,
    LostNoDbFec,
    LostSidecarDamaged,
}

fn assert_expected_class(
    name: &str,
    classification: &RecoveryClassification,
    expected: ExpectedClass,
) {
    match expected {
        ExpectedClass::Recovered => {
            assert!(
                matches!(classification, RecoveryClassification::Recovered { .. }),
                "scenario '{name}' must classify as Recovered, got: {classification:?}"
            );
        }
        ExpectedClass::LostInsufficientOrMissingSymbols => {
            assert!(
                matches!(
                    classification,
                    RecoveryClassification::Lost {
                        reason: fsqlite_e2e::recovery_runner::LostReason::InsufficientSymbols { .. }
                    } | RecoveryClassification::Lost {
                        reason: fsqlite_e2e::recovery_runner::LostReason::SidecarMissing
                    }
                ),
                "scenario '{name}' must classify as Lost(InsufficientSymbols|SidecarMissing), got: {classification:?}"
            );
        }
        ExpectedClass::LostRecoveryDisabled => {
            assert!(
                matches!(
                    classification,
                    RecoveryClassification::Lost {
                        reason: fsqlite_e2e::recovery_runner::LostReason::RecoveryDisabled
                    }
                ),
                "scenario '{name}' must classify as Lost(RecoveryDisabled), got: {classification:?}"
            );
        }
        ExpectedClass::LostNoDbFec => {
            assert!(
                matches!(
                    classification,
                    RecoveryClassification::Lost {
                        reason: fsqlite_e2e::recovery_runner::LostReason::NoDbFecAvailable
                    }
                ),
                "scenario '{name}' must classify as Lost(NoDbFecAvailable), got: {classification:?}"
            );
        }
        ExpectedClass::LostSidecarDamaged => {
            assert!(
                matches!(
                    classification,
                    RecoveryClassification::Lost {
                        reason: fsqlite_e2e::recovery_runner::LostReason::SidecarDamaged { .. }
                    } | RecoveryClassification::Lost {
                        reason: fsqlite_e2e::recovery_runner::LostReason::SidecarMissing
                    }
                ),
                "scenario '{name}' must classify as Lost(SidecarDamaged|SidecarMissing), got: {classification:?}"
            );
        }
    }
}

#[test]
fn test_bd_n0g4q_6_repair_matrix_and_evidence_cards() {
    reset_raptorq_repair_telemetry();

    // Seven corruption classes:
    // 1) single-page bit flip
    // 2) multi-page corruption within repair budget
    // 3) over-budget corruption (graceful failure path)
    // 4) recovery explicitly disabled
    // 5) database-page corruption (no DB-FEC available in this lane)
    // 6) sidecar corruption (graceful degradation path)
    // 7) WAL corruption without sidecar
    let matrix: [(&str, ExpectedClass, bool); 7] = [
        ("wal_single_bit_flip", ExpectedClass::Recovered, true),
        (
            "wal_corrupt_within_tolerance",
            ExpectedClass::Recovered,
            true,
        ),
        (
            "wal_corrupt_beyond_tolerance",
            ExpectedClass::LostInsufficientOrMissingSymbols,
            false,
        ),
        (
            "wal_corrupt_recovery_disabled",
            ExpectedClass::LostRecoveryDisabled,
            false,
        ),
        ("db_page_bitrot", ExpectedClass::LostNoDbFec, false),
        ("sidecar_damaged", ExpectedClass::LostSidecarDamaged, false),
        (
            "wal_corrupt_no_sidecar",
            ExpectedClass::LostInsufficientOrMissingSymbols,
            false,
        ),
    ];

    let mut recovered_count = 0_usize;
    let mut repair_evidence_count = 0_usize;

    for (name, expected, must_have_repairs) in matrix {
        let scenario = scenario_by_name(name);
        let report = run_recovery(&scenario);

        assert!(
            report.matches_expected,
            "scenario '{}' must match expected outcome; verdict={}",
            name, report.verdict
        );
        assert_expected_class(name, &report.classification, expected);

        if matches!(
            report.classification,
            RecoveryClassification::Recovered { .. }
        ) {
            recovered_count = recovered_count.saturating_add(1);
        }

        if must_have_repairs {
            assert!(
                !report.evidence.repairs.is_empty(),
                "scenario '{name}' must emit repair evidence entries"
            );
            assert!(
                report
                    .evidence
                    .integrity_checks
                    .iter()
                    .any(|check| check.passed),
                "scenario '{name}' must include at least one passing integrity check"
            );
            repair_evidence_count =
                repair_evidence_count.saturating_add(report.evidence.repairs.len());
        }
    }

    // Ensure we actually exercised successful repair paths.
    assert!(
        recovered_count >= 2,
        "matrix should include at least two successful recoveries"
    );
    assert!(
        repair_evidence_count > 0,
        "matrix should emit repair evidence for recoverable cases"
    );

    // Evidence cards are the append-only repair ledger entries with BLAKE3 witnesses.
    let cards = raptorq_repair_evidence_snapshot(0);
    assert!(
        !cards.is_empty(),
        "repair evidence ledger should contain at least one card"
    );
    for card in &cards {
        assert_ne!(card.chain_hash, [0_u8; 32], "chain_hash must be non-zero");
        assert_ne!(
            card.witness.corrupted_hash_blake3, [0_u8; 32],
            "corrupted witness hash must be non-zero"
        );
        assert_ne!(
            card.witness.repaired_hash_blake3, [0_u8; 32],
            "repaired witness hash must be non-zero"
        );
        assert_ne!(
            card.witness.expected_hash_blake3, [0_u8; 32],
            "expected witness hash must be non-zero"
        );
    }
}
