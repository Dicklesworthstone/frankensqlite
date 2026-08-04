//! Baseline generation test (bd-1lsfu.1).
//!
//! Run with:
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_1lsfu_1_generate_baseline -- --ignored --nocapture
//! ```
//!
//! This writes `baselines/operations/bd-1lsfu.1-baseline.json` to the
//! workspace root.
#![recursion_limit = "512"]

use fsqlite_e2e::baseline::{
    BaselineReport, LatencyStats, Operation, OperationBaseline, save_baseline,
};
use fsqlite_types::value::SqliteValue;

const ROW_COUNT: i64 = 1000;
/// Warmup and iteration counts.
///
/// In release mode (`--release`), use WARMUP=100 / ITERATIONS=1000 for
/// statistically robust baselines matching the bead spec.  In debug mode
/// (the default test profile), we use lower values to keep CI times
/// reasonable while still producing meaningful p50/p95/p99 distributions.
const WARMUP: u32 = 10;
const ITERATIONS: u32 = 100;

/// Async mirror of `fsqlite_e2e::baseline::measure_operation`: identical warmup /
/// measurement / percentile / throughput logic, but the measured operation is a
/// future that must be awaited inside the timing window (the engine API is async,
/// so `.await` cannot appear inside the plain `FnMut()` the sync helper takes).
async fn measure_operation_async<F: AsyncFnMut()>(
    warmup: u32,
    iterations: u32,
    mut f: F,
) -> (LatencyStats, f64) {
    // Warmup phase.
    for _ in 0..warmup {
        f().await;
    }

    // Measurement phase.
    let mut samples_micros: Vec<u64> = Vec::with_capacity(iterations as usize);
    for _ in 0..iterations {
        let start = std::time::Instant::now();
        f().await;
        let elapsed = start.elapsed();
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        samples_micros.push(micros);
    }

    samples_micros.sort_unstable();

    let len = samples_micros.len();
    let p50 = percentile(&samples_micros, 50);
    let p95 = percentile(&samples_micros, 95);
    let p99 = percentile(&samples_micros, 99);
    let max = samples_micros.last().copied().unwrap_or(0);

    // Throughput: median ops/sec based on p50.
    let throughput = if p50 > 0 {
        1_000_000.0 / p50 as f64
    } else if len > 0 {
        // Sub-microsecond: estimate from total time.
        let total_micros: u64 = samples_micros.iter().sum();
        if total_micros > 0 {
            (len as f64) * 1_000_000.0 / total_micros as f64
        } else {
            f64::INFINITY
        }
    } else {
        0.0
    };

    (
        LatencyStats {
            p50_micros: p50,
            p95_micros: p95,
            p99_micros: p99,
            max_micros: max,
        },
        throughput,
    )
}

/// Nearest-rank percentile on a sorted slice (mirrors the baseline module's).
fn percentile(sorted: &[u64], pct: u32) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let last_index = sorted.len() - 1;
    let pct_usize = usize::try_from(pct).map_or(100, |value| value.min(100));
    let idx = pct_usize.saturating_mul(last_index).saturating_add(50) / 100;
    sorted[idx.min(sorted.len() - 1)]
}

async fn setup_frankensqlite() -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(":memory:").await.unwrap();
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        drop(conn.execute(pragma).await);
    }
    conn.execute(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT NOT NULL,\
             category TEXT NOT NULL,\
             score INTEGER NOT NULL\
         )",
    )
    .await
    .unwrap();
    conn.execute("BEGIN").await.unwrap();
    for i in 1..=ROW_COUNT {
        conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', 'cat_{}', {})",
            i % 10,
            i * 7,
        ))
        .await
        .unwrap();
    }
    conn.execute("COMMIT").await.unwrap();

    // Second table for join.
    conn.execute(
        "CREATE TABLE bench2 (\
             id INTEGER PRIMARY KEY,\
             bench_id INTEGER NOT NULL,\
             label TEXT NOT NULL\
         )",
    )
    .await
    .unwrap();
    conn.execute("BEGIN").await.unwrap();
    for i in 1..=500_i64 {
        conn.execute(&format!(
            "INSERT INTO bench2 VALUES ({i}, {}, 'label_{i}')",
            i * 2,
        ))
        .await
        .unwrap();
    }
    conn.execute("COMMIT").await.unwrap();
    conn
}

#[allow(clippy::too_many_lines)]
async fn capture_baseline(engine: &str, conn: &fsqlite::Connection) -> Vec<OperationBaseline> {
    let mut baselines = Vec::new();

    // 1. Sequential scan.
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        let rows = conn.query("SELECT * FROM bench").await.unwrap();
        assert_eq!(
            i64::try_from(rows.len()).expect("row count must fit i64"),
            ROW_COUNT
        );
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::SequentialScan,
        engine: engine.to_owned(),
        row_count: ROW_COUNT as u64,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 2. Point lookup.
    let mut id = 1_i64;
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        let rows = conn
            .query(&format!("SELECT * FROM bench WHERE id = {id}"))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        id = (id % ROW_COUNT) + 1;
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::PointLookup,
        engine: engine.to_owned(),
        row_count: ROW_COUNT as u64,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 3. Range scan.
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        let rows = conn
            .query("SELECT * FROM bench WHERE id >= 100 AND id < 200")
            .await
            .unwrap();
        assert_eq!(rows.len(), 100);
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::RangeScan,
        engine: engine.to_owned(),
        row_count: ROW_COUNT as u64,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 4. Single-row insert.
    let ins_conn = fsqlite::Connection::open(":memory:").await.unwrap();
    ins_conn
        .execute("CREATE TABLE ins_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    let mut ins_id = 1_i64;
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        ins_conn
            .execute(&format!(
                "INSERT INTO ins_test VALUES ({ins_id}, 'val_{ins_id}')"
            ))
            .await
            .unwrap();
        ins_id += 1;
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::SingleRowInsert,
        engine: engine.to_owned(),
        row_count: 0,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 5. Batch insert.
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        let batch_conn = fsqlite::Connection::open(":memory:").await.unwrap();
        batch_conn
            .execute("CREATE TABLE batch_t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        batch_conn.execute("BEGIN").await.unwrap();
        for j in 1..=100_i64 {
            batch_conn
                .execute(&format!("INSERT INTO batch_t VALUES ({j}, 'v{j}')"))
                .await
                .unwrap();
        }
        batch_conn.execute("COMMIT").await.unwrap();
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::BatchInsert,
        engine: engine.to_owned(),
        row_count: 100,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 6. Single-row update.
    let mut upd_id = 1_i64;
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        conn.execute(&format!(
            "UPDATE bench SET score = {} WHERE id = {upd_id}",
            upd_id * 13,
        ))
        .await
        .unwrap();
        upd_id = (upd_id % ROW_COUNT) + 1;
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::SingleRowUpdate,
        engine: engine.to_owned(),
        row_count: ROW_COUNT as u64,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 7. Single-row delete.
    let del_conn = fsqlite::Connection::open(":memory:").await.unwrap();
    del_conn
        .execute("CREATE TABLE del_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    for j in 1..=10_000_i64 {
        del_conn
            .execute(&format!("INSERT INTO del_test VALUES ({j}, 'v{j}')"))
            .await
            .unwrap();
    }
    let mut del_id = 1_i64;
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        del_conn
            .execute(&format!("DELETE FROM del_test WHERE id = {del_id}"))
            .await
            .unwrap();
        del_id += 1;
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::SingleRowDelete,
        engine: engine.to_owned(),
        row_count: ROW_COUNT as u64,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 8. 2-way equi-join.
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        let rows = conn
            .query(
                "SELECT bench.id, bench.name, bench2.label \
                 FROM bench INNER JOIN bench2 ON bench.id = bench2.bench_id",
            )
            .await
            .unwrap();
        assert!(!rows.is_empty());
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::TwoWayEquiJoin,
        engine: engine.to_owned(),
        row_count: ROW_COUNT as u64,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 9. Aggregation.
    let (lat, thr) = measure_operation_async(WARMUP, ITERATIONS, async || {
        let rows = conn
            .query("SELECT COUNT(*), SUM(score), AVG(score) FROM bench")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));
    })
    .await;
    baselines.push(OperationBaseline {
        operation: Operation::Aggregation,
        engine: engine.to_owned(),
        row_count: ROW_COUNT as u64,
        iterations: ITERATIONS,
        warmup_iterations: WARMUP,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    baselines
}

/// Generate the initial baseline JSON artifact.
///
/// This test is `#[ignore]`d by default because it takes ~30 seconds
/// and produces a file artifact. Run it explicitly to refresh baselines.
#[test]
#[ignore = "baseline artifact generation is long-running and writes files"]
fn generate_operation_baseline() {
    asupersync::test_utils::run_test(|| async {
        let conn = setup_frankensqlite().await;
        let baselines = capture_baseline("frankensqlite", &conn).await;
        assert_eq!(baselines.len(), 9, "must capture all 9 operations");

        let mut report = BaselineReport::new("release");
        report.baselines = baselines;

        // Print summary.
        for b in &report.baselines {
            println!(
                "  {:20} p50={:>6}us  p95={:>6}us  p99={:>6}us  max={:>6}us  thr={:.0} ops/s",
                b.operation.display_name(),
                b.latency.p50_micros,
                b.latency.p95_micros,
                b.latency.p99_micros,
                b.latency.max_micros,
                b.throughput_ops_per_sec,
            );
        }

        // Save to baselines directory.
        let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let baseline_path = workspace_root.join("baselines/operations/bd-1lsfu.1-baseline.json");
        save_baseline(&report, &baseline_path).unwrap();
        println!("\nBaseline saved to: {}", baseline_path.display());

        // Verify it loads back.
        let loaded = fsqlite_e2e::baseline::load_baseline(&baseline_path).unwrap();
        assert_eq!(loaded.baselines.len(), 9);
    });
}

/// Quick smoke test (not ignored) that just verifies the baseline module
/// can measure all 9 operations without panicking.
#[test]
fn smoke_all_nine_operations_measurable() {
    asupersync::test_utils::run_test(|| async {
        let conn = fsqlite::Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=50_i64 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'n{i}', {i})"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, t_id INTEGER, label TEXT)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=25_i64 {
            conn.execute(&format!("INSERT INTO t2 VALUES ({i}, {}, 'l{i}')", i * 2))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        // Just verify no panics with minimal iterations.
        let w = 1_u32;
        let n = 3_u32;

        // 1. Sequential scan
        let (s, _) = measure_operation_async(w, n, async || {
            let _ = conn.query("SELECT * FROM t").await.unwrap();
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);

        // 2. Point lookup
        let (s, _) = measure_operation_async(w, n, async || {
            let _ = conn.query("SELECT * FROM t WHERE id = 1").await.unwrap();
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);

        // 3. Range scan
        let (s, _) = measure_operation_async(w, n, async || {
            let _ = conn
                .query("SELECT * FROM t WHERE id >= 10 AND id < 20")
                .await
                .unwrap();
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);

        // 4. Single-row insert
        let c4 = fsqlite::Connection::open(":memory:").await.unwrap();
        c4.execute("CREATE TABLE ins (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        let mut ins_id = 1_i64;
        let (s, _) = measure_operation_async(w, n, async || {
            c4.execute(&format!("INSERT INTO ins VALUES ({ins_id})"))
                .await
                .unwrap();
            ins_id += 1;
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);

        // 5. Batch insert
        let (s, _) = measure_operation_async(w, n, async || {
            let bc = fsqlite::Connection::open(":memory:").await.unwrap();
            bc.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            bc.execute("BEGIN").await.unwrap();
            for j in 1..=10_i64 {
                bc.execute(&format!("INSERT INTO b VALUES ({j})"))
                    .await
                    .unwrap();
            }
            bc.execute("COMMIT").await.unwrap();
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);

        // 6. Single-row update
        let (s, _) = measure_operation_async(w, n, async || {
            conn.execute("UPDATE t SET score = 99 WHERE id = 1")
                .await
                .unwrap();
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);

        // 7. Single-row delete
        let c7 = fsqlite::Connection::open(":memory:").await.unwrap();
        c7.execute("CREATE TABLE d (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        for j in 1..=100_i64 {
            c7.execute(&format!("INSERT INTO d VALUES ({j})"))
                .await
                .unwrap();
        }
        let mut did = 1_i64;
        let (s, _) = measure_operation_async(w, n, async || {
            c7.execute(&format!("DELETE FROM d WHERE id = {did}"))
                .await
                .unwrap();
            did += 1;
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);

        // 8. 2-way equi-join
        let (s, _) = measure_operation_async(w, n, async || {
            let _ = conn
                .query("SELECT t.id, t2.label FROM t INNER JOIN t2 ON t.id = t2.t_id")
                .await
                .unwrap();
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);

        // 9. Aggregation
        let (s, _) = measure_operation_async(w, n, async || {
            let _ = conn
                .query("SELECT COUNT(*), SUM(score), AVG(score) FROM t")
                .await
                .unwrap();
        })
        .await;
        assert!(s.max_micros >= s.p50_micros);
    });
}
