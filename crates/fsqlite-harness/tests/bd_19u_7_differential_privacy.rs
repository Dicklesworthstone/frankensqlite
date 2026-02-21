//! bd-19u.7: Differential privacy for aggregate queries integration tests.
//!
//! Validates the differential privacy framework:
//!   1. Privacy budget creation and enforcement
//!   2. Laplace mechanism noise calibration
//!   3. Gaussian mechanism noise calibration
//!   4. Budget exhaustion error handling
//!   5. Deterministic noise (same seed = same output)
//!   6. Laplace statistical properties (mean ~0, variance ~2b^2)
//!   7. Gaussian statistical properties (mean ~0, variance ~sigma^2)
//!   8. Sensitivity helpers (count, sum, avg)
//!   9. Global metrics lifecycle
//!  10. Machine-readable conformance output

use fsqlite_mvcc::{
    DpEngine, DpError, NoiseMechanism, PrivacyBudget, dp_metrics, reset_dp_metrics, sensitivity,
};

// ---------------------------------------------------------------------------
// Test 1: Privacy budget creation and enforcement
// ---------------------------------------------------------------------------

#[test]
fn test_budget_creation_enforcement() {
    let b = PrivacyBudget::new(2.0).unwrap();
    assert_eq!(b.total(), 2.0);
    assert_eq!(b.remaining(), 2.0);
    assert_eq!(b.spent(), 0.0);
    assert_eq!(b.queries_charged(), 0);

    // Invalid budgets.
    assert!(PrivacyBudget::new(0.0).is_err());
    assert!(PrivacyBudget::new(-1.0).is_err());
    assert!(PrivacyBudget::new(f64::NAN).is_err());
    assert!(PrivacyBudget::new(f64::INFINITY).is_err());

    println!("[PASS] budget creation: valid and invalid cases handled");
}

// ---------------------------------------------------------------------------
// Test 2: Laplace mechanism noise calibration
// ---------------------------------------------------------------------------

#[test]
fn test_laplace_noise_calibration() {
    let mut engine = DpEngine::new(10.0, 42).unwrap();

    // COUNT query: true=1000, sensitivity=1, epsilon=0.5.
    // Scale b = 1/0.5 = 2.
    let result = engine.laplace(1000.0, 1.0, 0.5).unwrap();
    assert_eq!(result.sensitivity, 1.0);
    assert_eq!(result.epsilon_spent, 0.5);
    assert_eq!(result.noise_scale, 2.0); // sensitivity/epsilon = 1/0.5
    assert_eq!(result.mechanism, NoiseMechanism::Laplace);

    // SUM query: true=50000, sensitivity=100, epsilon=1.0.
    // Scale b = 100/1 = 100.
    let result2 = engine.laplace(50000.0, 100.0, 1.0).unwrap();
    assert_eq!(result2.noise_scale, 100.0);

    // Budget tracking: 0.5 + 1.0 = 1.5 spent.
    let b = engine.budget();
    assert!((b.spent() - 1.5).abs() < 1e-10);
    assert_eq!(b.queries_charged(), 2);

    println!(
        "[PASS] Laplace calibration: b=2 for ε=0.5, b=100 for Δf=100/ε=1"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Gaussian mechanism noise calibration
// ---------------------------------------------------------------------------

#[test]
fn test_gaussian_noise_calibration() {
    let mut engine = DpEngine::new(10.0, 99).unwrap();

    // sensitivity=1, epsilon=1.0, delta=1e-5.
    // sigma = 1 * sqrt(2 * ln(1.25/1e-5)) / 1.0.
    let result = engine.gaussian(500.0, 1.0, 1.0, 1e-5).unwrap();
    assert_eq!(result.sensitivity, 1.0);
    assert_eq!(result.epsilon_spent, 1.0);

    // Expected sigma = sqrt(2*ln(125000)) = sqrt(2*11.736) ≈ 4.845.
    let expected_sigma = (2.0 * (1.25_f64 / 1e-5).ln()).sqrt();
    assert!(
        (result.noise_scale - expected_sigma).abs() < 0.01,
        "sigma should be ~{expected_sigma:.3}, got {:.3}",
        result.noise_scale
    );

    assert!(matches!(
        result.mechanism,
        NoiseMechanism::Gaussian { delta } if (delta - 1e-5).abs() < 1e-15
    ));

    println!(
        "[PASS] Gaussian calibration: sigma={:.4} for ε=1, δ=1e-5",
        result.noise_scale
    );
}

// ---------------------------------------------------------------------------
// Test 4: Budget exhaustion
// ---------------------------------------------------------------------------

#[test]
fn test_budget_exhaustion() {
    let mut engine = DpEngine::new(1.0, 42).unwrap();

    // Spend 0.6.
    engine.laplace(100.0, 1.0, 0.6).unwrap();
    // Spend 0.3 (total 0.9).
    engine.laplace(100.0, 1.0, 0.3).unwrap();

    // 0.1 remaining; requesting 0.5 should fail.
    let err = engine.laplace(100.0, 1.0, 0.5).unwrap_err();
    match &err {
        DpError::BudgetExhausted {
            requested,
            remaining,
        } => {
            assert!((requested - 0.5).abs() < 1e-10);
            assert!((remaining - 0.1).abs() < 1e-10);
        }
        other => panic!("Expected BudgetExhausted, got {other:?}"),
    }

    // 0.1 remaining; requesting 0.1 should succeed.
    engine.laplace(100.0, 1.0, 0.1).unwrap();

    // Now fully exhausted.
    let err2 = engine.laplace(100.0, 1.0, 0.01).unwrap_err();
    assert!(matches!(err2, DpError::BudgetExhausted { .. }));

    println!("[PASS] budget exhaustion: sequential composition enforced");
}

// ---------------------------------------------------------------------------
// Test 5: Deterministic noise
// ---------------------------------------------------------------------------

#[test]
fn test_deterministic_noise() {
    let mut e1 = DpEngine::new(10.0, 12345).unwrap();
    let mut e2 = DpEngine::new(10.0, 12345).unwrap();

    // Same seed → same noise for Laplace.
    let r1 = e1.laplace(100.0, 1.0, 1.0).unwrap();
    let r2 = e2.laplace(100.0, 1.0, 1.0).unwrap();
    assert!(
        (r1.noisy_value - r2.noisy_value).abs() < 1e-10,
        "same seed should produce identical Laplace noise"
    );

    // Same seed → same noise for Gaussian.
    let g1 = e1.gaussian(200.0, 2.0, 0.5, 1e-5).unwrap();
    let g2 = e2.gaussian(200.0, 2.0, 0.5, 1e-5).unwrap();
    assert!(
        (g1.noisy_value - g2.noisy_value).abs() < 1e-10,
        "same seed should produce identical Gaussian noise"
    );

    // Different seed → different noise.
    let mut e3 = DpEngine::new(10.0, 99999).unwrap();
    let r3 = e3.laplace(100.0, 1.0, 1.0).unwrap();
    assert!(
        (r1.noisy_value - r3.noisy_value).abs() > 1e-10,
        "different seeds should produce different noise"
    );

    println!(
        "[PASS] deterministic noise: same-seed={:.6}, diff-seed={:.6}",
        r1.noisy_value, r3.noisy_value
    );
}

// ---------------------------------------------------------------------------
// Test 6: Laplace statistical properties
// ---------------------------------------------------------------------------

#[test]
fn test_laplace_statistical_properties() {
    let b = 2.0; // scale parameter
    let n = 50_000;
    let mut engine = DpEngine::new(n as f64, 77777).unwrap();
    let mut sum_noise = 0.0;
    let mut sum_noise_sq = 0.0;

    for _ in 0..n {
        let result = engine.laplace(0.0, b, 1.0).unwrap(); // Lap(0, b/1) = Lap(0, 2)
        let noise = result.noisy_value;
        sum_noise += noise;
        sum_noise_sq += noise * noise;
    }

    let mean = sum_noise / n as f64;
    let variance = sum_noise_sq / n as f64 - mean * mean;
    let expected_variance = 2.0 * b * b; // Var[Lap(0, b)] = 2b²

    assert!(
        mean.abs() < 0.1,
        "Laplace mean should be ~0, got {mean:.4}"
    );
    assert!(
        (variance - expected_variance).abs() < 1.0,
        "Laplace variance should be ~{expected_variance:.1}, got {variance:.4}"
    );

    println!(
        "[PASS] Laplace stats (b={b}): mean={mean:.4}, var={variance:.2} (expected {expected_variance})"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Gaussian statistical properties
// ---------------------------------------------------------------------------

#[test]
fn test_gaussian_statistical_properties() {
    let sigma = 3.0;
    let n = 50_000;
    // We need a large enough budget: n queries at epsilon=sigma (trick to get sigma=1 * sqrt(...)/epsilon).
    // Simpler: just test the RNG distribution directly via repeated gaussian calls.
    let mut engine = DpEngine::new(n as f64 * 10.0, 88888).unwrap();
    let mut sum_noise = 0.0;
    let mut sum_noise_sq = 0.0;

    // We'll use sensitivity=sigma * epsilon / sqrt(2*ln(1.25/delta)).
    // With epsilon=1, delta=1e-5: sigma = sensitivity * sqrt(2*ln(125000)) / 1.
    // So sensitivity = sigma / sqrt(2*ln(125000)).
    let factor = (2.0 * (1.25_f64 / 1e-5).ln()).sqrt();
    let sens = sigma / factor;

    for _ in 0..n {
        let result = engine.gaussian(0.0, sens, 1.0, 1e-5).unwrap();
        let noise = result.noisy_value;
        sum_noise += noise;
        sum_noise_sq += noise * noise;
    }

    let mean = sum_noise / n as f64;
    let variance = sum_noise_sq / n as f64 - mean * mean;
    let expected_variance = sigma * sigma;

    assert!(
        mean.abs() < 0.2,
        "Gaussian mean should be ~0, got {mean:.4}"
    );
    assert!(
        (variance - expected_variance).abs() < 1.5,
        "Gaussian variance should be ~{expected_variance:.1}, got {variance:.4}"
    );

    println!(
        "[PASS] Gaussian stats (σ={sigma}): mean={mean:.4}, var={variance:.2} (expected {expected_variance})"
    );
}

// ---------------------------------------------------------------------------
// Test 8: Sensitivity helpers
// ---------------------------------------------------------------------------

#[test]
fn test_sensitivity_helpers() {
    assert_eq!(sensitivity::COUNT, 1.0);
    assert_eq!(sensitivity::sum(500.0), 500.0);

    // avg sensitivity = 2 * max / n.
    let s = sensitivity::avg(100.0, 1000);
    assert!((s - 0.2).abs() < 1e-10, "avg(100, 1000) = 0.2, got {s}");

    let s2 = sensitivity::avg(50.0, 100);
    assert!((s2 - 1.0).abs() < 1e-10, "avg(50, 100) = 1.0, got {s2}");

    // Edge case: n=0.
    assert_eq!(sensitivity::avg(100.0, 0), 0.0);

    println!("[PASS] sensitivity helpers: count={}, sum(500)={}, avg(100,1000)={s}", sensitivity::COUNT, sensitivity::sum(500.0));
}

// ---------------------------------------------------------------------------
// Test 9: Global metrics lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_lifecycle() {
    reset_dp_metrics();

    let m0 = dp_metrics();
    assert_eq!(m0.fsqlite_dp_queries_total, 0);
    assert_eq!(m0.fsqlite_dp_epsilon_spent_micros, 0);
    assert_eq!(m0.fsqlite_dp_budget_exhausted_total, 0);

    // Run some queries.
    let mut engine = DpEngine::new(5.0, 42).unwrap();
    engine.laplace(100.0, 1.0, 0.5).unwrap();
    engine.gaussian(200.0, 1.0, 0.3, 1e-5).unwrap();

    let m1 = dp_metrics();
    assert!(m1.fsqlite_dp_queries_total >= 2);
    // 0.5 + 0.3 = 0.8ε → 800000 micros.
    assert!(m1.fsqlite_dp_epsilon_spent_micros >= 800_000);
    assert_eq!(m1.fsqlite_dp_budget_exhausted_total, 0);

    // Trigger budget exhaustion.
    let mut engine2 = DpEngine::new(0.1, 42).unwrap();
    let _ = engine2.laplace(100.0, 1.0, 0.5); // should fail

    let m2 = dp_metrics();
    assert!(m2.fsqlite_dp_budget_exhausted_total >= 1, "exhaustion should be counted");

    // Serialization.
    let json = serde_json::to_string(&m2).unwrap();
    assert!(json.contains("fsqlite_dp_queries_total"));
    assert!(json.contains("fsqlite_dp_epsilon_spent_micros"));
    assert!(json.contains("fsqlite_dp_budget_exhausted_total"));

    println!(
        "[PASS] metrics: queries={} ε_micros={} exhausted={}",
        m2.fsqlite_dp_queries_total,
        m2.fsqlite_dp_epsilon_spent_micros,
        m2.fsqlite_dp_budget_exhausted_total
    );
}

// ---------------------------------------------------------------------------
// Test 10: Conformance summary (JSON)
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    struct TestResult {
        name: &'static str,
        pass: bool,
        detail: String,
    }

    let mut results = Vec::new();

    // 1. Budget creation.
    {
        let pass = PrivacyBudget::new(1.0).is_ok()
            && PrivacyBudget::new(0.0).is_err()
            && PrivacyBudget::new(-1.0).is_err();
        results.push(TestResult {
            name: "budget_creation",
            pass,
            detail: "valid/invalid accepted/rejected".to_string(),
        });
    }

    // 2. Laplace mechanism.
    {
        let mut e = DpEngine::new(10.0, 42).unwrap();
        let r = e.laplace(100.0, 1.0, 1.0).unwrap();
        let pass = r.mechanism == NoiseMechanism::Laplace && r.noise_scale == 1.0;
        results.push(TestResult {
            name: "laplace_mechanism",
            pass,
            detail: format!("scale={:.1}", r.noise_scale),
        });
    }

    // 3. Gaussian mechanism.
    {
        let mut e = DpEngine::new(10.0, 42).unwrap();
        let r = e.gaussian(100.0, 1.0, 1.0, 1e-5).unwrap();
        let pass = matches!(r.mechanism, NoiseMechanism::Gaussian { .. }) && r.noise_scale > 0.0;
        results.push(TestResult {
            name: "gaussian_mechanism",
            pass,
            detail: format!("sigma={:.4}", r.noise_scale),
        });
    }

    // 4. Budget exhaustion.
    {
        let mut e = DpEngine::new(0.5, 42).unwrap();
        e.laplace(100.0, 1.0, 0.5).unwrap();
        let pass = e.laplace(100.0, 1.0, 0.1).is_err();
        results.push(TestResult {
            name: "budget_exhaustion",
            pass,
            detail: "rejected over-budget query".to_string(),
        });
    }

    // 5. Deterministic.
    {
        let mut e1 = DpEngine::new(10.0, 42).unwrap();
        let mut e2 = DpEngine::new(10.0, 42).unwrap();
        let r1 = e1.laplace(100.0, 1.0, 1.0).unwrap();
        let r2 = e2.laplace(100.0, 1.0, 1.0).unwrap();
        let pass = (r1.noisy_value - r2.noisy_value).abs() < 1e-10;
        results.push(TestResult {
            name: "deterministic_noise",
            pass,
            detail: format!("v1={:.6} v2={:.6}", r1.noisy_value, r2.noisy_value),
        });
    }

    // 6. Metrics.
    {
        reset_dp_metrics();
        let mut e = DpEngine::new(10.0, 42).unwrap();
        e.laplace(100.0, 1.0, 1.0).unwrap();
        let m = dp_metrics();
        let pass = m.fsqlite_dp_queries_total >= 1 && m.fsqlite_dp_epsilon_spent_micros >= 1_000_000;
        results.push(TestResult {
            name: "metrics",
            pass,
            detail: format!("queries={} ε_micros={}", m.fsqlite_dp_queries_total, m.fsqlite_dp_epsilon_spent_micros),
        });
    }

    // Summary.
    let total = results.len();
    let passed = results.iter().filter(|r| r.pass).count();
    let failed = total - passed;

    println!("\n=== bd-19u.7: Differential Privacy Conformance Summary ===");
    println!("{{");
    println!("  \"bead\": \"bd-19u.7\",");
    println!("  \"suite\": \"differential_privacy\",");
    println!("  \"total\": {total},");
    println!("  \"passed\": {passed},");
    println!("  \"failed\": {failed},");
    println!(
        "  \"pass_rate\": \"{:.1}%\",",
        passed as f64 / total as f64 * 100.0
    );
    println!("  \"cases\": [");
    for (i, r) in results.iter().enumerate() {
        let comma = if i + 1 < total { "," } else { "" };
        let status = if r.pass { "PASS" } else { "FAIL" };
        println!(
            "    {{ \"name\": \"{}\", \"status\": \"{status}\", \"detail\": \"{}\" }}{comma}",
            r.name, r.detail
        );
    }
    println!("  ]");
    println!("}}");

    assert_eq!(
        failed, 0,
        "{failed}/{total} differential privacy conformance tests failed"
    );

    println!("[PASS] all {total} differential privacy conformance tests passed");
}
