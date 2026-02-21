//! bd-19u.4: NitroSketch probabilistic telemetry integration tests.
//!
//! Validates sub-linear memory data structures for runtime statistics:
//!   1. Count-Min Sketch basic frequency estimation
//!   2. Count-Min Sketch never-undercount guarantee
//!   3. Count-Min Sketch heavy-hitter accuracy under skew
//!   4. Streaming histogram latency distribution
//!   5. Streaming histogram percentile accuracy
//!   6. Global sketch metrics integration
//!   7. Memory tracking lifecycle (alloc/dealloc)
//!   8. Machine-readable conformance output

use fsqlite_mvcc::{
    CountMinSketch, CountMinSketchConfig, StreamingHistogram,
    reset_sketch_telemetry_metrics, sketch_telemetry_metrics,
};

// ---------------------------------------------------------------------------
// Test 1: CMS basic frequency estimation
// ---------------------------------------------------------------------------

#[test]
fn test_cms_basic_frequency() {
    let mut cms = CountMinSketch::new(&CountMinSketchConfig::default());

    // Observe various items with known frequencies.
    for _ in 0..100 {
        cms.observe(1);
    }
    for _ in 0..50 {
        cms.observe(2);
    }
    for _ in 0..10 {
        cms.observe(3);
    }
    cms.observe(999);

    assert_eq!(cms.estimate(1), 100, "item 1 should have freq 100");
    assert_eq!(cms.estimate(2), 50, "item 2 should have freq 50");
    assert_eq!(cms.estimate(3), 10, "item 3 should have freq 10");
    assert_eq!(cms.estimate(999), 1, "item 999 should have freq 1");
    assert_eq!(cms.total_count(), 161);

    println!("[PASS] CMS basic frequency: 4 items correct");
}

// ---------------------------------------------------------------------------
// Test 2: CMS never-undercount guarantee
// ---------------------------------------------------------------------------

#[test]
fn test_cms_never_undercounts() {
    let mut cms = CountMinSketch::new(&CountMinSketchConfig {
        width: 256,
        depth: 4,
        seed: 0xBEEF,
    });

    // Insert 500 distinct items with varying frequencies.
    for i in 0..500u64 {
        let freq = (i % 10) + 1;
        cms.observe_n(i, freq);
    }

    // Verify: estimate >= true count for all items.
    let mut violations = 0u64;
    for i in 0..500u64 {
        let true_count = (i % 10) + 1;
        let estimate = cms.estimate(i);
        if estimate < true_count {
            violations += 1;
        }
    }

    assert_eq!(
        violations, 0,
        "CMS must NEVER undercount: {violations} violations"
    );

    println!("[PASS] CMS never-undercount: 500 items, 0 violations");
}

// ---------------------------------------------------------------------------
// Test 3: CMS heavy-hitter accuracy under skew
// ---------------------------------------------------------------------------

#[test]
fn test_cms_heavy_hitter_skew() {
    let mut cms = CountMinSketch::new(&CountMinSketchConfig {
        width: 2048,
        depth: 4,
        seed: 0,
    });

    // Heavy hitter: item 42 with 50K observations.
    cms.observe_n(42, 50_000);

    // Light tail: 1000 items with 1 each.
    for i in 100..1100 {
        cms.observe(i);
    }

    let heavy_est = cms.estimate(42);
    assert_eq!(heavy_est, 50_000, "heavy hitter should be exactly 50K");

    // Light items should have modest overcount.
    let mut max_overcount = 0u64;
    for i in 100..1100 {
        let est = cms.estimate(i);
        assert!(est >= 1, "must not undercount light item {i}");
        max_overcount = max_overcount.max(est - 1);
    }

    // With width=2048 and depth=4, overcount should be very low.
    assert!(
        max_overcount <= 50,
        "max overcount {max_overcount} too high for width=2048"
    );

    println!(
        "[PASS] CMS heavy-hitter skew: heavy=50K exact, max_overcount={max_overcount}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Streaming histogram latency distribution
// ---------------------------------------------------------------------------

#[test]
fn test_histogram_latency_distribution() {
    let mut h = StreamingHistogram::new_latency_us();

    // Simulate a latency distribution: many fast, few slow.
    for _ in 0..1000 {
        h.observe(5); // 5µs — fast
    }
    for _ in 0..800 {
        h.observe(50); // 50µs — medium
    }
    for _ in 0..150 {
        h.observe(500); // 500µs — slow
    }
    for _ in 0..50 {
        h.observe(10_000); // 10ms — very slow
    }

    assert_eq!(h.count(), 2000);
    assert_eq!(h.min(), 5);
    assert_eq!(h.max(), 10_000);
    assert!(h.mean() > 0.0);

    // Verify bucket boundaries.
    assert_eq!(h.bucket_count(), 18, "default latency has 18 boundaries");

    println!(
        "[PASS] histogram latency: count={} min={}µs max={}µs mean={:.1}µs",
        h.count(),
        h.min(),
        h.max(),
        h.mean()
    );
}

// ---------------------------------------------------------------------------
// Test 5: Streaming histogram percentile accuracy
// ---------------------------------------------------------------------------

#[test]
fn test_histogram_percentile_accuracy() {
    let mut h = StreamingHistogram::new(&[100, 200, 500, 1000, 5000, 10000]);

    // Uniform distribution: 100 observations per bucket.
    for _ in 0..100 {
        h.observe(50); // <=100
    }
    for _ in 0..100 {
        h.observe(150); // <=200
    }
    for _ in 0..100 {
        h.observe(350); // <=500
    }
    for _ in 0..100 {
        h.observe(750); // <=1000
    }
    for _ in 0..100 {
        h.observe(3000); // <=5000
    }
    for _ in 0..100 {
        h.observe(8000); // <=10000
    }

    assert_eq!(h.count(), 600);

    let p50 = h.percentile(0.50);
    let p90 = h.percentile(0.90);
    let p99 = h.percentile(0.99);

    // p50 with 6 equal buckets should be boundary of 3rd bucket (500).
    assert_eq!(p50, 500, "p50 should be 500");
    // p90 should be boundary of 6th bucket (10000).
    assert_eq!(p90, 10000, "p90 should be 10000");
    // p99 should be boundary of 6th bucket (10000).
    assert_eq!(p99, 10000, "p99 should be 10000");

    println!("[PASS] histogram percentiles: p50={p50} p90={p90} p99={p99}");
}

// ---------------------------------------------------------------------------
// Test 6: Global sketch metrics integration
// ---------------------------------------------------------------------------

#[test]
fn test_sketch_metrics_integration() {
    reset_sketch_telemetry_metrics();

    let mut cms = CountMinSketch::new(&CountMinSketchConfig {
        width: 64,
        depth: 2,
        seed: 0,
    });

    cms.observe(1);
    cms.observe(2);
    cms.observe(3);
    _ = cms.estimate(1);
    _ = cms.estimate(2);

    let mut h = StreamingHistogram::new(&[100, 1000]);
    h.observe(50);
    h.observe(500);

    let m = sketch_telemetry_metrics();
    assert!(m.fsqlite_sketch_memory_bytes > 0, "memory gauge should be > 0");
    assert_eq!(m.fsqlite_sketch_observations_total, 5, "3 CMS + 2 histogram");
    assert_eq!(m.fsqlite_sketch_estimates_total, 2, "2 CMS estimates");

    // Verify serialization.
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("fsqlite_sketch_memory_bytes"));
    assert!(json.contains("fsqlite_sketch_estimates_total"));
    assert!(json.contains("fsqlite_sketch_observations_total"));

    println!(
        "[PASS] sketch metrics: mem={} obs={} est={}",
        m.fsqlite_sketch_memory_bytes,
        m.fsqlite_sketch_observations_total,
        m.fsqlite_sketch_estimates_total
    );
}

// ---------------------------------------------------------------------------
// Test 7: Memory tracking lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_memory_tracking_lifecycle() {
    reset_sketch_telemetry_metrics();

    let m0 = sketch_telemetry_metrics();
    assert_eq!(m0.fsqlite_sketch_memory_bytes, 0, "starts at zero");

    {
        let _cms = CountMinSketch::new(&CountMinSketchConfig {
            width: 128,
            depth: 2,
            seed: 0,
        });
        let m1 = sketch_telemetry_metrics();
        let expected_bytes = 128 * 2 * 8;
        assert_eq!(
            m1.fsqlite_sketch_memory_bytes, expected_bytes as u64,
            "CMS should add {}B",
            expected_bytes
        );

        {
            let _h = StreamingHistogram::new(&[100, 500, 1000]);
            let m2 = sketch_telemetry_metrics();
            assert!(
                m2.fsqlite_sketch_memory_bytes > m1.fsqlite_sketch_memory_bytes,
                "histogram should add memory"
            );
        }
        // Histogram dropped.
        let m3 = sketch_telemetry_metrics();
        assert_eq!(
            m3.fsqlite_sketch_memory_bytes, m1.fsqlite_sketch_memory_bytes,
            "histogram drop should restore to CMS-only level"
        );
    }
    // CMS dropped.
    let m4 = sketch_telemetry_metrics();
    assert_eq!(
        m4.fsqlite_sketch_memory_bytes, 0,
        "all dropped — memory gauge should be 0"
    );

    println!("[PASS] memory tracking lifecycle: alloc/dealloc balanced");
}

// ---------------------------------------------------------------------------
// Test 8: Conformance summary (JSON)
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    struct TestResult {
        name: &'static str,
        pass: bool,
        detail: String,
    }

    let mut results = Vec::new();

    // 1. CMS basic
    {
        let mut cms = CountMinSketch::new(&CountMinSketchConfig::default());
        cms.observe_n(1, 100);
        cms.observe(2);
        let pass = cms.estimate(1) == 100 && cms.estimate(2) == 1;
        results.push(TestResult {
            name: "cms_basic",
            pass,
            detail: format!("est(1)={} est(2)={}", cms.estimate(1), cms.estimate(2)),
        });
    }

    // 2. CMS never undercounts (quick)
    {
        let mut cms = CountMinSketch::new(&CountMinSketchConfig {
            width: 128,
            depth: 4,
            seed: 0,
        });
        let mut ok = true;
        for i in 0..100u64 {
            cms.observe_n(i, i + 1);
        }
        for i in 0..100u64 {
            if cms.estimate(i) < i + 1 {
                ok = false;
            }
        }
        results.push(TestResult {
            name: "cms_no_undercount",
            pass: ok,
            detail: format!("100 items checked"),
        });
    }

    // 3. Histogram basic
    {
        let mut h = StreamingHistogram::new(&[10, 100, 1000]);
        h.observe(5);
        h.observe(50);
        h.observe(500);
        let pass = h.count() == 3 && h.min() == 5 && h.max() == 500;
        results.push(TestResult {
            name: "histogram_basic",
            pass,
            detail: format!("count={} min={} max={}", h.count(), h.min(), h.max()),
        });
    }

    // 4. Histogram percentiles
    {
        let mut h = StreamingHistogram::new(&[10, 50, 100]);
        for _ in 0..50 {
            h.observe(5);
        }
        for _ in 0..50 {
            h.observe(75);
        }
        let p50 = h.percentile(0.50);
        let pass = p50 == 10; // first 50 are in <=10 bucket
        results.push(TestResult {
            name: "histogram_percentiles",
            pass,
            detail: format!("p50={p50}"),
        });
    }

    // 5. Metrics increment
    {
        reset_sketch_telemetry_metrics();
        let mut cms = CountMinSketch::new(&CountMinSketchConfig {
            width: 64,
            depth: 2,
            seed: 0,
        });
        cms.observe(1);
        _ = cms.estimate(1);
        let m = sketch_telemetry_metrics();
        let pass = m.fsqlite_sketch_observations_total >= 1
            && m.fsqlite_sketch_estimates_total >= 1;
        results.push(TestResult {
            name: "metrics_increment",
            pass,
            detail: format!(
                "obs={} est={}",
                m.fsqlite_sketch_observations_total, m.fsqlite_sketch_estimates_total
            ),
        });
    }

    // 6. Memory gauge
    {
        reset_sketch_telemetry_metrics();
        {
            let _cms = CountMinSketch::new(&CountMinSketchConfig {
                width: 64,
                depth: 2,
                seed: 0,
            });
            let m = sketch_telemetry_metrics();
            assert!(m.fsqlite_sketch_memory_bytes > 0);
        }
        let m = sketch_telemetry_metrics();
        let pass = m.fsqlite_sketch_memory_bytes == 0;
        results.push(TestResult {
            name: "memory_gauge",
            pass,
            detail: format!("after_drop={}", m.fsqlite_sketch_memory_bytes),
        });
    }

    // Summary
    let total = results.len();
    let passed = results.iter().filter(|r| r.pass).count();
    let failed = total - passed;

    println!("\n=== bd-19u.4: NitroSketch Telemetry Conformance Summary ===");
    println!("{{");
    println!("  \"bead\": \"bd-19u.4\",");
    println!("  \"suite\": \"nitrosketch_telemetry\",");
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
        "{failed}/{total} NitroSketch telemetry conformance tests failed"
    );

    println!("[PASS] all {total} NitroSketch telemetry conformance tests passed");
}
