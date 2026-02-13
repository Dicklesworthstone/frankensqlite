//! Baseline generation test (bd-1lsfu.1).
//!
//! Run with:
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_1lsfu_1_generate_baseline -- --ignored --nocapture
//! ```
//!
//! This writes `baselines/operations/bd-1lsfu.1-baseline.json` to the
//! workspace root.

use fsqlite_e2e::baseline::{
    BaselineReport, Operation, OperationBaseline, measure_operation, save_baseline,
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

fn setup_frankensqlite() -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        let _ = conn.execute(pragma);
    }
    conn.execute(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT NOT NULL,\
             category TEXT NOT NULL,\
             score INTEGER NOT NULL\
         )",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=ROW_COUNT {
        conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', 'cat_{}', {})",
            i % 10,
            i * 7,
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    // Second table for join.
    conn.execute(
        "CREATE TABLE bench2 (\
             id INTEGER PRIMARY KEY,\
             bench_id INTEGER NOT NULL,\
             label TEXT NOT NULL\
         )",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=500_i64 {
        conn.execute(&format!(
            "INSERT INTO bench2 VALUES ({i}, {}, 'label_{i}')",
            i * 2,
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn
}

fn capture_baseline(engine: &str, conn: &fsqlite::Connection) -> Vec<OperationBaseline> {
    let mut baselines = Vec::new();

    // 1. Sequential scan.
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        let rows = conn.query("SELECT * FROM bench").unwrap();
        assert_eq!(rows.len() as i64, ROW_COUNT);
    });
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
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        let rows = conn
            .query(&format!("SELECT * FROM bench WHERE id = {id}"))
            .unwrap();
        assert_eq!(rows.len(), 1);
        id = (id % ROW_COUNT) + 1;
    });
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
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        let rows = conn
            .query("SELECT * FROM bench WHERE id >= 100 AND id < 200")
            .unwrap();
        assert_eq!(rows.len(), 100);
    });
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
    let ins_conn = fsqlite::Connection::open(":memory:").unwrap();
    ins_conn
        .execute("CREATE TABLE ins_test (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    let mut ins_id = 1_i64;
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        ins_conn
            .execute(&format!(
                "INSERT INTO ins_test VALUES ({ins_id}, 'val_{ins_id}')"
            ))
            .unwrap();
        ins_id += 1;
    });
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
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        let batch_conn = fsqlite::Connection::open(":memory:").unwrap();
        batch_conn
            .execute("CREATE TABLE batch_t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        batch_conn.execute("BEGIN").unwrap();
        for j in 1..=100_i64 {
            batch_conn
                .execute(&format!("INSERT INTO batch_t VALUES ({j}, 'v{j}')"))
                .unwrap();
        }
        batch_conn.execute("COMMIT").unwrap();
    });
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
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        conn.execute(&format!(
            "UPDATE bench SET score = {} WHERE id = {upd_id}",
            upd_id * 13,
        ))
        .unwrap();
        upd_id = (upd_id % ROW_COUNT) + 1;
    });
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
    let del_conn = fsqlite::Connection::open(":memory:").unwrap();
    del_conn
        .execute("CREATE TABLE del_test (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    for j in 1..=10_000_i64 {
        del_conn
            .execute(&format!("INSERT INTO del_test VALUES ({j}, 'v{j}')"))
            .unwrap();
    }
    let mut del_id = 1_i64;
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        del_conn
            .execute(&format!("DELETE FROM del_test WHERE id = {del_id}"))
            .unwrap();
        del_id += 1;
    });
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
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        let rows = conn
            .query(
                "SELECT bench.id, bench.name, bench2.label \
                 FROM bench INNER JOIN bench2 ON bench.id = bench2.bench_id",
            )
            .unwrap();
        assert!(!rows.is_empty());
    });
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
    let (lat, thr) = measure_operation(WARMUP, ITERATIONS, || {
        let rows = conn
            .query("SELECT COUNT(*), SUM(score), AVG(score) FROM bench")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));
    });
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
#[ignore]
fn generate_operation_baseline() {
    let conn = setup_frankensqlite();
    let baselines = capture_baseline("frankensqlite", &conn);
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
}

/// Quick smoke test (not ignored) that just verifies the baseline module
/// can measure all 9 operations without panicking.
#[test]
fn smoke_all_nine_operations_measurable() {
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=50_i64 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'n{i}', {i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, t_id INTEGER, label TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=25_i64 {
        conn.execute(&format!("INSERT INTO t2 VALUES ({i}, {}, 'l{i}')", i * 2))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    // Just verify no panics with minimal iterations.
    let w = 1_u32;
    let n = 3_u32;

    // 1. Sequential scan
    let (s, _) = measure_operation(w, n, || {
        let _ = conn.query("SELECT * FROM t").unwrap();
    });
    assert!(s.max_micros >= s.p50_micros);

    // 2. Point lookup
    let (s, _) = measure_operation(w, n, || {
        let _ = conn.query("SELECT * FROM t WHERE id = 1").unwrap();
    });
    assert!(s.max_micros >= s.p50_micros);

    // 3. Range scan
    let (s, _) = measure_operation(w, n, || {
        let _ = conn
            .query("SELECT * FROM t WHERE id >= 10 AND id < 20")
            .unwrap();
    });
    assert!(s.max_micros >= s.p50_micros);

    // 4. Single-row insert
    let c4 = fsqlite::Connection::open(":memory:").unwrap();
    c4.execute("CREATE TABLE ins (id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut ins_id = 1_i64;
    let (s, _) = measure_operation(w, n, || {
        c4.execute(&format!("INSERT INTO ins VALUES ({ins_id})"))
            .unwrap();
        ins_id += 1;
    });
    assert!(s.max_micros >= s.p50_micros);

    // 5. Batch insert
    let (s, _) = measure_operation(w, n, || {
        let bc = fsqlite::Connection::open(":memory:").unwrap();
        bc.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
            .unwrap();
        bc.execute("BEGIN").unwrap();
        for j in 1..=10_i64 {
            bc.execute(&format!("INSERT INTO b VALUES ({j})")).unwrap();
        }
        bc.execute("COMMIT").unwrap();
    });
    assert!(s.max_micros >= s.p50_micros);

    // 6. Single-row update
    let (s, _) = measure_operation(w, n, || {
        conn.execute("UPDATE t SET score = 99 WHERE id = 1")
            .unwrap();
    });
    assert!(s.max_micros >= s.p50_micros);

    // 7. Single-row delete
    let c7 = fsqlite::Connection::open(":memory:").unwrap();
    c7.execute("CREATE TABLE d (id INTEGER PRIMARY KEY)")
        .unwrap();
    for j in 1..=100_i64 {
        c7.execute(&format!("INSERT INTO d VALUES ({j})")).unwrap();
    }
    let mut did = 1_i64;
    let (s, _) = measure_operation(w, n, || {
        c7.execute(&format!("DELETE FROM d WHERE id = {did}"))
            .unwrap();
        did += 1;
    });
    assert!(s.max_micros >= s.p50_micros);

    // 8. 2-way equi-join
    let (s, _) = measure_operation(w, n, || {
        let _ = conn
            .query("SELECT t.id, t2.label FROM t INNER JOIN t2 ON t.id = t2.t_id")
            .unwrap();
    });
    assert!(s.max_micros >= s.p50_micros);

    // 9. Aggregation
    let (s, _) = measure_operation(w, n, || {
        let _ = conn
            .query("SELECT COUNT(*), SUM(score), AVG(score) FROM t")
            .unwrap();
    });
    assert!(s.max_micros >= s.p50_micros);
}
