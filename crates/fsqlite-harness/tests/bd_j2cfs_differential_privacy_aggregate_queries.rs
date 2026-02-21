//! bd-j2cfs: Differential Privacy for aggregate queries (§12.5) — harness
//! integration tests.
//!
//! Extends bd-19u.7 (core mechanism validation) with:
//! - PRAGMA-level configuration model
//! - Per-session privacy budget management
//! - Utility analysis: accuracy vs privacy tradeoff at various epsilon values
//! - Sequential composition theorem verification
//! - Multi-aggregate query workflows (COUNT/SUM/AVG in sequence)
//! - Budget allocation strategies (uniform, proportional)
//! - Sensitivity bounds validation
//! - Error display and serialization
//! - Conformance summary

use fsqlite_mvcc::{
    DpEngine, DpError, NoiseMechanism, PrivacyBudget, dp_metrics,
    reset_dp_metrics, sensitivity,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Simulate PRAGMA differential_privacy configuration.
/// In a real integration, this would map to connection-level state.
struct DpPragmaConfig {
    enabled: bool,
    total_epsilon: f64,
    seed: u64,
}

impl DpPragmaConfig {
    fn on(epsilon: f64, seed: u64) -> Self {
        Self {
            enabled: true,
            total_epsilon: epsilon,
            seed,
        }
    }

    fn off() -> Self {
        Self {
            enabled: false,
            total_epsilon: 0.0,
            seed: 0,
        }
    }

    fn create_engine(&self) -> Option<DpEngine> {
        if self.enabled {
            DpEngine::new(self.total_epsilon, self.seed).ok()
        } else {
            None
        }
    }
}

/// Run a utility analysis: execute the same query at various epsilon values
/// and measure relative error.
fn utility_analysis(
    true_value: f64,
    sensitivity_val: f64,
    epsilons: &[f64],
    samples_per_epsilon: usize,
    seed_base: u64,
) -> Vec<(f64, f64)> {
    // Returns (epsilon, mean_relative_error).
    epsilons
        .iter()
        .map(|&eps| {
            let mut total_rel_err = 0.0;
            let budget = eps * samples_per_epsilon as f64;
            let mut engine = DpEngine::new(budget, seed_base + (eps * 1000.0) as u64).unwrap();

            for _ in 0..samples_per_epsilon {
                let result = engine.laplace(true_value, sensitivity_val, eps).unwrap();
                let rel_err = if true_value.abs() > f64::EPSILON {
                    (result.noisy_value - true_value).abs() / true_value.abs()
                } else {
                    result.noisy_value.abs()
                };
                total_rel_err += rel_err;
            }

            let mean_rel_err = total_rel_err / samples_per_epsilon as f64;
            (eps, mean_rel_err)
        })
        .collect()
}

// ── 1. PRAGMA configuration model ────────────────────────────────────────────

#[test]
fn pragma_configuration_model() {
    // PRAGMA differential_privacy = ON with budget.
    let cfg_on = DpPragmaConfig::on(2.0, 42);
    assert!(cfg_on.enabled);
    assert_eq!(cfg_on.total_epsilon, 2.0);
    let engine = cfg_on.create_engine();
    assert!(engine.is_some());
    let e = engine.unwrap();
    assert_eq!(e.budget().total(), 2.0);
    assert_eq!(e.budget().remaining(), 2.0);

    // PRAGMA differential_privacy = OFF.
    let cfg_off = DpPragmaConfig::off();
    assert!(!cfg_off.enabled);
    assert!(cfg_off.create_engine().is_none());
}

// ── 2. Per-session budget isolation ──────────────────────────────────────────

#[test]
fn per_session_budget_isolation() {
    // Two independent sessions should not interfere.
    let mut session_a = DpEngine::new(5.0, 100).unwrap();
    let mut session_b = DpEngine::new(3.0, 200).unwrap();

    session_a.laplace(1000.0, 1.0, 2.0).unwrap();
    assert!((session_a.budget().spent() - 2.0).abs() < 1e-10);
    assert!((session_b.budget().spent() - 0.0).abs() < 1e-10);

    session_b.laplace(500.0, 1.0, 1.0).unwrap();
    assert!((session_a.budget().spent() - 2.0).abs() < 1e-10);
    assert!((session_b.budget().spent() - 1.0).abs() < 1e-10);
}

// ── 3. Utility analysis: accuracy improves with higher epsilon ───────────────

#[test]
fn utility_accuracy_improves_with_epsilon() {
    let true_value = 10_000.0;
    let sens = sensitivity::COUNT; // 1.0
    let epsilons = [0.01, 0.1, 0.5, 1.0, 2.0, 5.0];
    let samples = 1000;

    let results = utility_analysis(true_value, sens, &epsilons, samples, 42);

    // Higher epsilon → lower relative error (more accuracy, less privacy).
    for i in 1..results.len() {
        assert!(
            results[i].1 <= results[i - 1].1 * 1.5, // allow some statistical noise
            "epsilon {:.2} should have <= error than {:.2}: {:.6} vs {:.6}",
            results[i].0,
            results[i - 1].0,
            results[i].1,
            results[i - 1].1
        );
    }

    // At epsilon=5.0, relative error on COUNT of 10000 should be small.
    let (_, err_at_5) = results.last().unwrap();
    assert!(
        *err_at_5 < 0.01,
        "at ε=5.0, mean relative error should be <1%, got {:.4}%",
        err_at_5 * 100.0
    );
}

// ── 4. Sequential composition: total privacy loss = sum of epsilons ──────────

#[test]
fn sequential_composition_budget_tracking() {
    let mut engine = DpEngine::new(3.0, 42).unwrap();

    // Three queries each spending ε=1.0.
    let _r1 = engine.laplace(100.0, 1.0, 1.0).unwrap();
    let _r2 = engine.laplace(200.0, 1.0, 1.0).unwrap();
    let _r3 = engine.laplace(300.0, 1.0, 1.0).unwrap();

    // Total spent: 3.0 (sequential composition).
    assert!((engine.budget().spent() - 3.0).abs() < 1e-10);
    assert_eq!(engine.budget().queries_charged(), 3);

    // Budget fully exhausted.
    assert!(!engine.budget().can_spend(0.01));
    let err = engine.laplace(400.0, 1.0, 0.01).unwrap_err();
    assert!(matches!(err, DpError::BudgetExhausted { .. }));
}

// ── 5. Multi-aggregate workflow: COUNT + SUM + AVG in one session ────────────

#[test]
fn multi_aggregate_workflow() {
    let mut engine = DpEngine::new(3.0, 42).unwrap();

    // COUNT query: sensitivity=1, epsilon=1.0.
    let count_result = engine.laplace(1000.0, sensitivity::COUNT, 1.0).unwrap();
    assert_eq!(count_result.sensitivity, 1.0);
    assert_eq!(count_result.noise_scale, 1.0); // 1/1 = 1

    // SUM query: max contribution 500, epsilon=1.0.
    let sum_result = engine.laplace(50000.0, sensitivity::sum(500.0), 1.0).unwrap();
    assert_eq!(sum_result.sensitivity, 500.0);
    assert_eq!(sum_result.noise_scale, 500.0); // 500/1 = 500

    // AVG query: max contribution 500, n=1000, epsilon=1.0.
    let avg_sens = sensitivity::avg(500.0, 1000);
    let avg_result = engine.laplace(50.0, avg_sens, 1.0).unwrap();
    assert!((avg_result.sensitivity - 1.0).abs() < 1e-10); // 2*500/1000 = 1.0
    assert!((avg_result.noise_scale - 1.0).abs() < 1e-10);

    // Total budget spent: 3.0.
    assert!((engine.budget().spent() - 3.0).abs() < 1e-10);
}

// ── 6. Uniform budget allocation ─────────────────────────────────────────────

#[test]
fn uniform_budget_allocation() {
    let total_budget = 4.0;
    let num_queries = 4;
    let per_query_epsilon = total_budget / num_queries as f64;

    let mut engine = DpEngine::new(total_budget, 42).unwrap();

    for i in 0..num_queries {
        let result = engine
            .laplace(100.0 * (i + 1) as f64, 1.0, per_query_epsilon)
            .unwrap();
        assert_eq!(result.epsilon_spent, per_query_epsilon);
    }

    assert_eq!(engine.budget().queries_charged(), 4);
    assert!((engine.budget().spent() - total_budget).abs() < 1e-10);
}

// ── 7. Proportional budget allocation ────────────────────────────────────────

#[test]
fn proportional_budget_allocation() {
    // Allocate more budget to higher-sensitivity queries.
    let total_budget: f64 = 3.0;
    let count_eps: f64 = 0.5; // low sensitivity → small budget
    let sum_eps: f64 = 2.0; // high sensitivity → larger budget
    let avg_eps: f64 = 0.5;

    assert!(
        (count_eps + sum_eps + avg_eps - total_budget).abs() < 1e-10,
        "allocations should sum to total budget"
    );

    let mut engine = DpEngine::new(total_budget, 42).unwrap();

    let r1 = engine.laplace(1000.0, sensitivity::COUNT, count_eps).unwrap();
    assert_eq!(r1.noise_scale, sensitivity::COUNT / count_eps); // 1/0.5 = 2

    let r2 = engine
        .laplace(50000.0, sensitivity::sum(1000.0), sum_eps)
        .unwrap();
    assert_eq!(r2.noise_scale, 1000.0 / sum_eps); // 500

    let avg_sens = sensitivity::avg(100.0, 500);
    let r3 = engine.laplace(200.0, avg_sens, avg_eps).unwrap();
    assert!((r3.noise_scale - avg_sens / avg_eps).abs() < 1e-10);

    assert!((engine.budget().spent() - total_budget).abs() < 1e-10);
}

// ── 8. Sensitivity bounds: COUNT always 1, SUM depends on max ────────────────

#[test]
fn sensitivity_bounds_validation() {
    assert_eq!(sensitivity::COUNT, 1.0, "COUNT sensitivity is always 1");
    assert_eq!(sensitivity::sum(0.0), 0.0);
    assert_eq!(sensitivity::sum(100.0), 100.0);
    assert_eq!(sensitivity::sum(1e6), 1e6);

    // AVG sensitivity decreases with more rows.
    let avg_10 = sensitivity::avg(100.0, 10);
    let avg_100 = sensitivity::avg(100.0, 100);
    let avg_1000 = sensitivity::avg(100.0, 1000);
    assert!(avg_10 > avg_100);
    assert!(avg_100 > avg_1000);
    assert!((avg_10 - 20.0).abs() < 1e-10); // 2*100/10
    assert!((avg_100 - 2.0).abs() < 1e-10); // 2*100/100
    assert!((avg_1000 - 0.2).abs() < 1e-10); // 2*100/1000

    // Edge case: 0 rows.
    assert_eq!(sensitivity::avg(100.0, 0), 0.0);
}

// ── 9. DpError display ───────────────────────────────────────────────────────

#[test]
fn dp_error_display() {
    let budget_err = DpError::BudgetExhausted {
        requested: 1.5,
        remaining: 0.3,
    };
    let msg = budget_err.to_string();
    assert!(msg.contains("1.5"), "should show requested epsilon");
    assert!(msg.contains("0.3"), "should show remaining epsilon");
    assert!(msg.contains("exhausted"));

    let param_err = DpError::InvalidParameter("bad value".to_string());
    assert!(param_err.to_string().contains("bad value"));
}

// ── 10. DpQueryResult fields ─────────────────────────────────────────────────

#[test]
fn dp_query_result_fields() {
    let mut engine = DpEngine::new(10.0, 42).unwrap();

    let laplace_r = engine.laplace(100.0, 2.0, 0.5).unwrap();
    assert_eq!(laplace_r.epsilon_spent, 0.5);
    assert_eq!(laplace_r.sensitivity, 2.0);
    assert_eq!(laplace_r.noise_scale, 4.0); // 2.0/0.5
    assert_eq!(laplace_r.mechanism, NoiseMechanism::Laplace);
    // noisy_value should be near 100 but not exactly 100.
    assert!(laplace_r.noisy_value != 100.0);

    let gaussian_r = engine.gaussian(200.0, 3.0, 1.0, 1e-5).unwrap();
    assert_eq!(gaussian_r.epsilon_spent, 1.0);
    assert_eq!(gaussian_r.sensitivity, 3.0);
    assert!(gaussian_r.noise_scale > 0.0);
    assert!(matches!(
        gaussian_r.mechanism,
        NoiseMechanism::Gaussian { .. }
    ));
}

// ── 11. NoiseMechanism display ───────────────────────────────────────────────

#[test]
fn noise_mechanism_display() {
    assert_eq!(NoiseMechanism::Laplace.to_string(), "Laplace");

    let gauss = NoiseMechanism::Gaussian { delta: 1e-5 };
    let s = gauss.to_string();
    assert!(s.contains("Gaussian"));
    assert!(s.contains("1.00e-5") || s.contains("1e-5") || s.contains("1.0e-5"));
}

// ── 12. DpMetrics serialization ──────────────────────────────────────────────

#[test]
fn dp_metrics_serialization() {
    reset_dp_metrics();
    let mut engine = DpEngine::new(5.0, 42).unwrap();
    engine.laplace(100.0, 1.0, 0.5).unwrap();

    let m = dp_metrics();
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("fsqlite_dp_queries_total"));
    assert!(json.contains("fsqlite_dp_epsilon_spent_micros"));
    assert!(json.contains("fsqlite_dp_budget_exhausted_total"));

    // Deserialize back.
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(parsed["fsqlite_dp_queries_total"].as_u64().unwrap() >= 1);
}

// ── 13. Invalid parameter rejection ──────────────────────────────────────────

#[test]
fn invalid_parameter_rejection() {
    // Invalid epsilon for engine creation.
    assert!(DpEngine::new(0.0, 42).is_err());
    assert!(DpEngine::new(-1.0, 42).is_err());
    assert!(DpEngine::new(f64::NAN, 42).is_err());
    assert!(DpEngine::new(f64::INFINITY, 42).is_err());

    // Invalid sensitivity/epsilon in queries.
    let mut engine = DpEngine::new(10.0, 42).unwrap();
    assert!(engine.laplace(100.0, -1.0, 1.0).is_err()); // negative sensitivity
    assert!(engine.laplace(100.0, 1.0, -1.0).is_err()); // negative epsilon
    assert!(engine.laplace(100.0, 0.0, 1.0).is_err()); // zero sensitivity
    assert!(engine.laplace(100.0, 1.0, 0.0).is_err()); // zero epsilon

    // Invalid delta in Gaussian.
    assert!(engine.gaussian(100.0, 1.0, 1.0, 0.0).is_err()); // delta=0
    assert!(engine.gaussian(100.0, 1.0, 1.0, 1.0).is_err()); // delta=1
    assert!(engine.gaussian(100.0, 1.0, 1.0, -0.5).is_err()); // negative delta
}

// ── 14. Gaussian noise scale formula ─────────────────────────────────────────

#[test]
fn gaussian_noise_scale_formula() {
    let mut engine = DpEngine::new(10.0, 42).unwrap();

    let sens = 2.0;
    let eps = 1.0;
    let delta = 1e-5;

    let result = engine.gaussian(100.0, sens, eps, delta).unwrap();

    // sigma = sensitivity * sqrt(2 * ln(1.25 / delta)) / epsilon.
    let expected_sigma = sens * (2.0 * (1.25_f64 / delta).ln()).sqrt() / eps;
    assert!(
        (result.noise_scale - expected_sigma).abs() < 1e-10,
        "sigma={} expected={expected_sigma}",
        result.noise_scale
    );
}

// ── 15. PrivacyBudget can_spend ──────────────────────────────────────────────

#[test]
fn privacy_budget_can_spend() {
    let b = PrivacyBudget::new(1.0).unwrap();
    assert!(b.can_spend(0.5));
    assert!(b.can_spend(1.0));
    assert!(!b.can_spend(1.1));
    assert!(!b.can_spend(0.0)); // zero epsilon not valid
    assert!(!b.can_spend(-0.5)); // negative epsilon not valid
}

// ── 16. Laplace noise scale formula ──────────────────────────────────────────

#[test]
fn laplace_noise_scale_formula() {
    let mut engine = DpEngine::new(10.0, 42).unwrap();

    // scale b = sensitivity / epsilon.
    let r1 = engine.laplace(0.0, 1.0, 1.0).unwrap();
    assert_eq!(r1.noise_scale, 1.0);

    let r2 = engine.laplace(0.0, 10.0, 2.0).unwrap();
    assert_eq!(r2.noise_scale, 5.0); // 10/2

    let r3 = engine.laplace(0.0, 0.5, 0.25).unwrap();
    assert_eq!(r3.noise_scale, 2.0); // 0.5/0.25
}

// ── 17. Budget nearly exhausted edge cases ───────────────────────────────────

#[test]
fn budget_nearly_exhausted_edge_cases() {
    let mut engine = DpEngine::new(1.0, 42).unwrap();

    // Spend exactly 1.0 in one query.
    engine.laplace(100.0, 1.0, 1.0).unwrap();
    assert!((engine.budget().remaining() - 0.0).abs() < 1e-10);

    // Any further query with reasonable epsilon should fail.
    let err = engine.laplace(100.0, 1.0, 0.01).unwrap_err();
    assert!(matches!(err, DpError::BudgetExhausted { .. }));

    // Budget is fully exhausted.
    assert!(!engine.budget().can_spend(0.001));
}

// ── Conformance summary ─────────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    // Gate 1: PRAGMA config model.
    let cfg = DpPragmaConfig::on(1.0, 42);
    assert!(cfg.create_engine().is_some());

    // Gate 2: Budget isolation.
    let mut e1 = DpEngine::new(5.0, 1).unwrap();
    let e2 = DpEngine::new(3.0, 2).unwrap();
    e1.laplace(100.0, 1.0, 1.0).unwrap();
    assert_eq!(e2.budget().spent(), 0.0);

    // Gate 3: Composition.
    let mut e3 = DpEngine::new(2.0, 42).unwrap();
    e3.laplace(100.0, 1.0, 1.0).unwrap();
    e3.laplace(100.0, 1.0, 1.0).unwrap();
    assert!((e3.budget().spent() - 2.0).abs() < 1e-10);

    // Gate 4: Multi-aggregate.
    let mut e4 = DpEngine::new(3.0, 42).unwrap();
    e4.laplace(1000.0, sensitivity::COUNT, 1.0).unwrap();
    e4.laplace(50000.0, sensitivity::sum(100.0), 1.0).unwrap();
    e4.laplace(50.0, sensitivity::avg(100.0, 100), 1.0).unwrap();
    assert_eq!(e4.budget().queries_charged(), 3);

    // Gate 5: Sensitivity bounds.
    assert_eq!(sensitivity::COUNT, 1.0);
    assert!(sensitivity::avg(100.0, 10) > sensitivity::avg(100.0, 100));

    // Gate 6: Error handling.
    assert!(DpEngine::new(0.0, 42).is_err());
    let mut e5 = DpEngine::new(0.1, 42).unwrap();
    assert!(e5.laplace(100.0, 1.0, 1.0).is_err());

    let total_gates = 6;
    let passed = 6;
    println!("[bd-j2cfs] conformance: {passed}/{total_gates} gates passed");
}
