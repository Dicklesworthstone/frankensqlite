use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use asupersync::types::Budget;
use fsqlite_harness::fslab::FsLab;
use fsqlite_mvcc::{
    ConformalCalibratorConfig, ConformalOracleCalibrator, InvariantScore, OracleReport, Section,
    check_sheaf_consistency,
};

const BEAD_ID: &str = "bd-3go.6";
const TXN_COUNT: usize = 20;
const CALIBRATION_SEEDS: u64 = 100;
const HOLDOUT_SEEDS: u64 = 100;
const INVARIANT_NAME: &str = "mvcc_sheaf_consistency";

#[allow(clippy::cast_precision_loss)]
fn normal_score(seed: u64) -> f64 {
    let bucket = (seed.wrapping_mul(37).wrapping_add(13)) % 100;
    bucket as f64 / 100.0
}

fn make_consistent_sections(seed: u64) -> Vec<Section> {
    let base_version = (seed % 10_000).wrapping_mul(10);

    (0..TXN_COUNT)
        .map(|txn_idx| {
            let page_a = u64::try_from((txn_idx % 5) + 1).expect("page fits u64");
            let page_b = u64::try_from(((txn_idx + 2) % 5) + 1).expect("page fits u64");

            let mut observations = HashMap::new();
            observations.insert(page_a, base_version + page_a);
            observations.insert(page_b, base_version + page_b);

            Section {
                txn_id: u64::try_from(txn_idx + 1).expect("txn id fits u64"),
                observations,
            }
        })
        .collect()
}

fn make_inconsistent_sections(seed: u64) -> Vec<Section> {
    let mut sections = make_consistent_sections(seed);

    if let Some(first_section) = sections.get_mut(0) {
        first_section.observations.insert(1, u64::MAX - 7);
    }

    sections
}

fn run_lab_with_20_concurrent_txns(seed: u64) {
    let lab = FsLab::new(seed).worker_count(4).max_steps(50_000);
    let completions = Arc::new(Mutex::new(Vec::with_capacity(TXN_COUNT)));

    let report = lab.run_with_setup(|runtime, root| {
        for txn in 0..TXN_COUNT {
            let completions = Arc::clone(&completions);
            let txn_u64 = u64::try_from(txn).expect("txn fits in u64");

            let (task_id, _task_handle) = runtime
                .state
                .create_task(root, Budget::INFINITE, async move {
                    let signature = txn_u64.wrapping_mul(17).wrapping_add(3);
                    completions
                        .lock()
                        .expect("completion lock not poisoned")
                        .push(signature);
                })
                .expect("lab task creation must succeed");

            runtime
                .scheduler
                .lock()
                .expect("scheduler lock not poisoned")
                .schedule(task_id, u8::try_from(txn % 4).expect("priority fits in u8"));
        }
    });

    assert!(
        report.oracle_report.all_passed(),
        "bead_id={BEAD_ID} seed={seed} oracle report failed: {:?}",
        report.oracle_report
    );
    assert!(
        report.invariant_violations.is_empty(),
        "bead_id={BEAD_ID} seed={seed} invariant violations: {:?}",
        report.invariant_violations
    );
    assert!(
        report.quiescent,
        "bead_id={BEAD_ID} seed={seed} runtime must be quiescent"
    );
    assert!(
        report.steps_total > 0,
        "bead_id={BEAD_ID} seed={seed} expected non-zero scheduler work"
    );

    let completion_count = completions
        .lock()
        .expect("completion lock not poisoned")
        .len();
    assert_eq!(
        completion_count, TXN_COUNT,
        "bead_id={BEAD_ID} seed={seed} expected all transaction tasks to complete"
    );
}

#[test]
fn test_e2e_sheaf_plus_conformal_mvcc_verification() {
    let mut calibrator = ConformalOracleCalibrator::new(ConformalCalibratorConfig {
        alpha: 0.05,
        min_calibration_samples: 50,
    });

    for seed in 0..CALIBRATION_SEEDS {
        run_lab_with_20_concurrent_txns(seed);

        let sections = make_consistent_sections(seed);
        let sheaf_result = check_sheaf_consistency(&sections, None);
        assert!(
            sheaf_result.is_consistent(),
            "bead_id={BEAD_ID} seed={seed} expected sheaf-consistent sections"
        );

        calibrator.calibrate(&OracleReport {
            scores: vec![InvariantScore {
                invariant: INVARIANT_NAME.to_string(),
                score: normal_score(seed),
            }],
        });
    }

    assert!(
        calibrator.is_calibrated(),
        "bead_id={BEAD_ID} calibrator should be ready after 100 seeds"
    );

    let mut conforming_count = 0usize;
    for seed in CALIBRATION_SEEDS..(CALIBRATION_SEEDS + HOLDOUT_SEEDS) {
        let prediction = calibrator
            .predict(&OracleReport {
                scores: vec![InvariantScore {
                    invariant: INVARIANT_NAME.to_string(),
                    score: normal_score(seed),
                }],
            })
            .expect("prediction should be available once calibrated");

        if prediction.prediction_sets[0].conforming {
            conforming_count += 1;
        }
    }

    #[allow(clippy::cast_precision_loss)]
    let conforming_rate = conforming_count as f64 / HOLDOUT_SEEDS as f64;
    assert!(
        conforming_rate >= 0.95,
        "bead_id={BEAD_ID} expected >=95% conforming holdout rate, got {conforming_rate:.3}"
    );

    let inconsistent_sections = make_inconsistent_sections(0x0BAD_5EED);
    let inconsistent_result = check_sheaf_consistency(&inconsistent_sections, None);
    assert!(
        !inconsistent_result.is_consistent(),
        "bead_id={BEAD_ID} injected inconsistency must be detected"
    );

    let anomaly_prediction = calibrator
        .predict(&OracleReport {
            scores: vec![InvariantScore {
                invariant: INVARIANT_NAME.to_string(),
                score: 10.0,
            }],
        })
        .expect("prediction should be available once calibrated");

    assert!(
        !anomaly_prediction.prediction_sets[0].conforming,
        "bead_id={BEAD_ID} expected anomaly score to be non-conforming"
    );
}
