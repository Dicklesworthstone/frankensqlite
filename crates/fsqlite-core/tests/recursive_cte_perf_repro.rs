//! Performance regression repro for recursive CTEs.
//!
//! Bench observation: `WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL
//! SELECT x+1 FROM s WHERE x<N) SELECT SUM(x) FROM s` showed O(n^2)
//! behaviour, with n=1000 taking ~24 ms vs C SQLite's 142 us.
//!
//! This test measures total query time at several sizes and asserts
//! the scaling is sub-quadratic.

use std::time::Instant;

use fsqlite_core::connection::Connection;

async fn run_at(n: i64) -> std::time::Duration {
    let conn = Connection::open(":memory:").await.unwrap();
    let sql = format!(
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < {n}) SELECT SUM(x) FROM cnt"
    );
    let stmt = conn.prepare(&sql).await.unwrap();
    // Warm caches (parse + compile + first run).
    let _ = stmt.query_row().await;
    // Take median of several runs to reduce noise.
    let mut samples = Vec::new();
    for _ in 0..5 {
        let t0 = Instant::now();
        let _ = stmt.query_row().await;
        samples.push(t0.elapsed());
    }
    samples.sort();
    samples[samples.len() / 2]
}

#[test]
fn recursive_cte_sum_scales_linearly() {
    asupersync::test_utils::run_test(|| async {
    // Collect timings for N = 100, 300, 900.
    let t100 = run_at(100).await;
    let t300 = run_at(300).await;
    let t900 = run_at(900).await;
    eprintln!(
        "recursive_cte_sum timings: N=100 {:?}, N=300 {:?}, N=900 {:?}",
        t100, t300, t900
    );

    // If the algorithm is O(n^2), the ratio t900/t100 should be ~81x.
    // For O(n) we expect ~9x. Allow up to ~25x to be conservative
    // about constant-factor noise and allocation jitter; this still
    // distinguishes the regression decisively (was >70x before fix).
    let ratio = t900.as_nanos() as f64 / t100.as_nanos().max(1) as f64;
    eprintln!("ratio t900/t100 = {ratio:.2}");
    assert!(
        ratio < 25.0,
        "recursive CTE scaling looks quadratic: ratio t900/t100 = {ratio:.2} \
         (t100={:?}, t900={:?})",
        t100,
        t900
    );
    });
}
