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

async fn run_count_at(n: i64) -> std::time::Duration {
    let conn = Connection::open(":memory:").await.unwrap();
    // COUNT defeats the closed-form integer-series SUM specialization, so
    // this exercises the general recursive frontier executor — the path the
    // SUM test above stopped guarding once that specialization landed
    // (bd-gpi5i).
    let sql = format!(
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < {n}) SELECT COUNT(*) FROM cnt"
    );
    let stmt = conn.prepare(&sql).await.unwrap();
    let _ = stmt.query_row().await;
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
fn recursive_cte_general_count_scales_linearly() {
    asupersync::test_utils::run_test(|| async {
        let t100 = run_count_at(100).await;
        let t300 = run_count_at(300).await;
        let t900 = run_count_at(900).await;
        eprintln!(
            "recursive_cte_general_count timings: N=100 {t100:?}, N=300 {t300:?}, N=900 {t900:?}"
        );
        let ratio = t900.as_nanos() as f64 / t100.as_nanos().max(1) as f64;
        eprintln!("ratio t900/t100 = {ratio:.2}");
        assert!(
            ratio < 25.0,
            "general recursive frontier scaling looks quadratic: ratio t900/t100 = {ratio:.2} \
         (t100={t100:?}, t900={t900:?})"
        );
    });
}

/// Cross-engine oracle parity for the sync Direct frontier lane (bd-gpi5i):
/// every arm expression stays inside the sync node set (Column/Literal/
/// BinaryOp/UnaryOp/IsNull/numbered placeholder), and the CAST twin forces
/// the async lane — FrankenSQLite must agree with C SQLite on all of them.
#[test]
fn recursive_cte_sync_direct_lane_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let arm_exprs = [
            "x+1",
            "-x",
            "+x",
            "x*2-3",
            "NOT x",
            "x IS NULL",
            "(x % 7) + 1",
            "x IS TRUE",
            // Leaves the sync node set: async lane parity guard.
            "CAST(x+1 AS INTEGER)",
        ];
        for arm_expr in arm_exprs {
            let sql = format!(
                "WITH RECURSIVE cnt(x, y) AS (\
                   SELECT 1, 0 \
                   UNION ALL \
                   SELECT x+1, {arm_expr} FROM cnt WHERE x < 40\
                 ) SELECT COUNT(*), MIN(y), MAX(y), SUM(y) FROM cnt"
            );

            let oracle = rusqlite::Connection::open_in_memory().unwrap();
            let expected: (i64, Option<i64>, Option<i64>, Option<i64>) = oracle
                .query_row(&sql, [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .unwrap();

            let conn = Connection::open(":memory:").await.unwrap();
            let stmt = conn.prepare(&sql).await.unwrap();
            let row = stmt.query_row().await.unwrap();
            let got_count = match row.get(0).unwrap() {
                fsqlite_types::SqliteValue::Integer(v) => *v,
                other => panic!("count not integer for {arm_expr}: {other:?}"),
            };
            let get_opt = |idx: usize| -> Option<i64> {
                match row.get(idx).unwrap() {
                    fsqlite_types::SqliteValue::Integer(v) => Some(*v),
                    fsqlite_types::SqliteValue::Null => None,
                    other => panic!("agg {idx} not integer/null for {arm_expr}: {other:?}"),
                }
            };
            assert_eq!(
                (got_count, get_opt(1), get_opt(2), get_opt(3)),
                expected,
                "recursive-CTE arm `{arm_expr}` diverged from the C SQLite oracle"
            );
        }
    });
}

/// Placeholder rebinding through the sync lane: the bound value must be
/// re-read per execution (no stale plan capture).
#[test]
fn recursive_cte_sync_lane_placeholder_rebinds() {
    asupersync::test_utils::run_test(|| async {
        let sql = "WITH RECURSIVE cnt(x) AS (\
                     SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < ?1\
                   ) SELECT COUNT(*) FROM cnt";
        let conn = Connection::open(":memory:").await.unwrap();
        let stmt = conn.prepare(sql).await.unwrap();
        for bound in [10_i64, 25, 40, 10] {
            let row = stmt
                .query_row_with_params(&[fsqlite_types::SqliteValue::Integer(bound)])
                .await
                .unwrap();
            assert_eq!(
                row.get(0).unwrap(),
                &fsqlite_types::SqliteValue::Integer(bound),
                "COUNT for bound {bound} must equal the bound"
            );
        }
    });
}
