//! Bead bd-n0g4q.6: E2E corruption/repair matrix with evidence validation.
//!
//! This suite exercises seven deterministic corruption classes using the
//! recovery runner, then validates:
//! - expected recovery classification per scenario,
//! - presence/quality of structured repair evidence,
//! - content-derived success witnesses and honest no-payload failure markers,
//! - non-zero chain hashes in the evidence ledger.

use fsqlite_e2e::corruption_scenarios::{CorruptionScenario, scenario_catalog};
use fsqlite_e2e::recovery_runner::{RecoveryClassification, run_recovery};
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
                            | fsqlite_e2e::recovery_runner::LostReason::SidecarMissing
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
                            | fsqlite_e2e::recovery_runner::LostReason::SidecarMissing
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
    let mut verified_success_cards = 0_usize;
    let mut verified_failure_cards = 0_usize;
    let mut verified_unobserved_cards = 0_usize;

    for (name, expected, must_have_repairs) in matrix {
        let previous_cards = raptorq_repair_evidence_snapshot(0).len();
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

        // Bind each newly appended card to the actual scenario outcome. A
        // failed decode deliberately has no repaired payload; requiring a
        // nonzero repaired hash there would demand fabricated evidence.
        let cards = raptorq_repair_evidence_snapshot(0);
        assert!(cards.len() >= previous_cards, "unexpected ledger eviction");
        let new_cards = &cards[previous_cards..];
        if must_have_repairs {
            assert!(!new_cards.is_empty(), "scenario '{name}' emitted no card");
        }
        for card in new_cards {
            let log = report
                .wal_recovery_log
                .as_ref()
                .expect("a repair card must have an actual recovery log");
            assert_eq!(card.group_id, log.group_id, "scenario '{name}'");
            assert_ne!(card.chain_hash, [0_u8; 32], "scenario '{name}'");
            if log.required_symbols == 0 {
                // Disabled recovery or missing metadata can stop before any
                // expected/recovered content is available. These cards must
                // record that absence, not manufacture nonzero witnesses.
                assert!(matches!(
                    report.classification,
                    RecoveryClassification::Lost { .. }
                ));
                assert!(!log.outcome_is_recovered);
                assert!(!log.decode_attempted);
                assert_eq!(log.available_symbols, 0);
                assert_eq!(
                    card.witness,
                    fsqlite_wal::WalFecRepairWitnessTriple::zeroed()
                );
                verified_unobserved_cards += 1;
                continue;
            }
            assert_ne!(card.witness.corrupted_hash_blake3, [0_u8; 32]);
            assert_ne!(card.witness.expected_hash_blake3, [0_u8; 32]);
            assert_ne!(
                card.witness.corrupted_hash_blake3, card.witness.expected_hash_blake3,
                "scenario '{name}' must witness actual damaged bytes"
            );
            if matches!(
                report.classification,
                RecoveryClassification::Recovered { .. }
            ) {
                assert!(log.outcome_is_recovered);
                assert_ne!(card.witness.repaired_hash_blake3, [0_u8; 32]);
                assert_eq!(
                    card.witness.repaired_hash_blake3, card.witness.expected_hash_blake3,
                    "scenario '{name}' must restore the expected content"
                );
                verified_success_cards += 1;
            } else {
                assert!(!log.outcome_is_recovered);
                assert_eq!(
                    card.witness.repaired_hash_blake3, [0_u8; 32],
                    "scenario '{name}' must not claim a recovered payload"
                );
                verified_failure_cards += 1;
            }
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

    assert!(
        verified_success_cards >= 2,
        "successful repair cards missing"
    );
    assert!(verified_failure_cards >= 1, "failed repair card missing");
    assert!(verified_unobserved_cards >= 1, "early bailout card missing");
}
