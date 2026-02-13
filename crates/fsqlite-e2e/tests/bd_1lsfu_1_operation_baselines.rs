//! Integration tests for operation baseline capture and regression detection.
//!
//! Bead: bd-1lsfu.1
//!
//! Tests that:
//! 1. All 9 operations can be measured via `measure_operation`.
//! 2. Baseline JSON roundtrips correctly.
//! 3. Regression detection works with configurable thresholds.
//! 4. Baselines can be captured for both FrankenSQLite and C SQLite.

use fsqlite_e2e::baseline::{
    BaselineReport, DEFAULT_REGRESSION_THRESHOLD, LatencyStats, Operation, OperationBaseline,
    RegressionResult, measure_operation,
};

// ─── Baseline module unit integration tests ─────────────────────────────

#[test]
fn all_nine_operations_have_unique_names() {
    let ops = Operation::all();
    assert_eq!(ops.len(), 9);
    let mut names: Vec<&str> = ops.iter().map(|o| o.display_name()).collect();
    let orig_len = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), orig_len, "duplicate operation display names");
}

#[test]
fn baseline_report_json_schema_version() {
    let report = BaselineReport::new("test");
    let json = report.to_pretty_json().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        parsed["schema_version"],
        "fsqlite-e2e.operation_baseline.v1"
    );
    assert!(parsed["methodology"]["version"].is_string());
    assert!(parsed["environment"]["arch"].is_string());
}

#[test]
fn regression_threshold_default_is_ten_percent() {
    assert!((DEFAULT_REGRESSION_THRESHOLD - 0.10).abs() < f64::EPSILON);
}

#[test]
fn regression_check_empty_reports() {
    let old = BaselineReport::new("test");
    let current = BaselineReport::new("test");
    let results = old.check_regression(&current, DEFAULT_REGRESSION_THRESHOLD);
    assert!(results.is_empty());
}

#[test]
fn regression_check_missing_operation_in_current() {
    let mut old = BaselineReport::new("test");
    old.baselines.push(OperationBaseline {
        operation: Operation::PointLookup,
        engine: "frankensqlite".to_owned(),
        row_count: 1000,
        iterations: 100,
        warmup_iterations: 10,
        latency: LatencyStats {
            p50_micros: 50,
            p95_micros: 100,
            p99_micros: 200,
            max_micros: 500,
        },
        throughput_ops_per_sec: 20000.0,
    });

    let current = BaselineReport::new("test");
    let results = old.check_regression(&current, DEFAULT_REGRESSION_THRESHOLD);
    // Missing operation = no comparison = no regression.
    assert!(results.is_empty());
}

#[test]
fn regression_check_exact_match() {
    let baseline = OperationBaseline {
        operation: Operation::Aggregation,
        engine: "frankensqlite".to_owned(),
        row_count: 1000,
        iterations: 100,
        warmup_iterations: 10,
        latency: LatencyStats {
            p50_micros: 100,
            p95_micros: 200,
            p99_micros: 300,
            max_micros: 500,
        },
        throughput_ops_per_sec: 10000.0,
    };

    let mut old = BaselineReport::new("test");
    old.baselines.push(baseline.clone());
    let mut current = BaselineReport::new("test");
    current.baselines.push(baseline);

    let results = old.check_regression(&current, DEFAULT_REGRESSION_THRESHOLD);
    assert_eq!(results.len(), 1);
    assert!(!results[0].regressed);
    assert!((results[0].change_pct).abs() < 0.01);
}

#[test]
fn regression_result_summary_contains_key_info() {
    let result = RegressionResult {
        operation: Operation::BatchInsert,
        engine: "frankensqlite".to_owned(),
        baseline_p50_micros: 1000,
        current_p50_micros: 1200,
        change_pct: 20.0,
        regressed: true,
    };
    let summary = result.summary();
    assert!(summary.contains("REGRESSION"));
    assert!(summary.contains("batch_insert"));
    assert!(summary.contains("frankensqlite"));
    assert!(summary.contains("1000"));
    assert!(summary.contains("1200"));
}

// ─── Live operation measurement tests ───────────────────────────────────

#[test]
fn measure_noop_operation() {
    let (stats, throughput) = measure_operation(2, 10, || {
        // No-op: should be very fast.
    });
    // Stats should be valid (non-panicking, sane values).
    assert!(stats.p50_micros <= stats.p95_micros);
    assert!(stats.p95_micros <= stats.p99_micros);
    assert!(stats.p99_micros <= stats.max_micros);
    assert!(throughput > 0.0);
}

#[test]
fn measure_frankensqlite_point_lookup() {
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..100_i64 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let mut id = 1_i64;
    let (stats, throughput) = measure_operation(5, 50, || {
        let rows = conn
            .query(&format!("SELECT * FROM t WHERE id = {id}"))
            .unwrap();
        assert_eq!(rows.len(), 1);
        id = (id % 100) + 1;
    });

    assert!(stats.p50_micros >= 1, "point lookup should take >= 1us");
    assert!(throughput > 0.0);
}

#[test]
fn measure_frankensqlite_sequential_scan() {
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..200_i64 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let (stats, throughput) = measure_operation(3, 20, || {
        let rows = conn.query("SELECT * FROM t").unwrap();
        assert_eq!(rows.len(), 200);
    });

    assert!(stats.p50_micros >= 1);
    assert!(throughput > 0.0);
}

#[test]
fn measure_csqlite_point_lookup() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
        .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO t VALUES (?1, ('v' || ?1))")
            .unwrap();
        for i in 0..100_i64 {
            stmt.execute(rusqlite::params![i]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let mut stmt = conn.prepare("SELECT * FROM t WHERE id = ?1").unwrap();
    let mut id = 1_i64;
    let (stats, throughput) = measure_operation(5, 50, || {
        let rows: Vec<(i64,)> = stmt
            .query_map(rusqlite::params![id], |row| Ok((row.get(0).unwrap(),)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 1);
        id = (id % 100) + 1;
    });

    // p50 is u64, always >= 0; just confirm stats are valid.
    assert!(stats.p95_micros >= stats.p50_micros);
    assert!(throughput > 0.0);
}

// ─── Baseline save/load integration ─────────────────────────────────────

#[test]
fn save_load_roundtrip_with_all_operations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_baseline.json");

    let mut report = BaselineReport::new("test");
    for op in Operation::all() {
        report.baselines.push(OperationBaseline {
            operation: op,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 100,
                p95_micros: 200,
                p99_micros: 300,
                max_micros: 500,
            },
            throughput_ops_per_sec: 10000.0,
        });
    }

    fsqlite_e2e::baseline::save_baseline(&report, &path).unwrap();
    let loaded = fsqlite_e2e::baseline::load_baseline(&path).unwrap();

    assert_eq!(loaded.baselines.len(), 9);
    for (i, op) in Operation::all().iter().enumerate() {
        assert_eq!(loaded.baselines[i].operation, *op);
    }
}

// ─── Full 9-operation baseline capture ──────────────────────────────────

#[test]
fn capture_all_nine_baselines_frankensqlite() {
    let conn = fsqlite::Connection::open(":memory:").unwrap();

    // Setup: create main table with 200 rows.
    conn.execute(
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, category TEXT, score INTEGER)",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=200_i64 {
        conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', 'cat_{}', {})",
            i % 10,
            i * 7,
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    // Setup: create join table with 100 rows.
    conn.execute("CREATE TABLE bench2 (id INTEGER PRIMARY KEY, bench_id INTEGER, label TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=100_i64 {
        conn.execute(&format!(
            "INSERT INTO bench2 VALUES ({i}, {}, 'label_{i}')",
            i * 2,
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let mut report = BaselineReport::new("test");
    let warmup = 3_u32;
    let iters = 20_u32;

    // 1. Sequential scan.
    let (lat, thr) = measure_operation(warmup, iters, || {
        let rows = conn.query("SELECT * FROM bench").unwrap();
        assert_eq!(rows.len(), 200);
    });
    report.baselines.push(OperationBaseline {
        operation: Operation::SequentialScan,
        engine: "frankensqlite".to_owned(),
        row_count: 200,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 2. Point lookup.
    let mut id = 1_i64;
    let (lat, thr) = measure_operation(warmup, iters, || {
        let rows = conn
            .query(&format!("SELECT * FROM bench WHERE id = {id}"))
            .unwrap();
        assert_eq!(rows.len(), 1);
        id = (id % 200) + 1;
    });
    report.baselines.push(OperationBaseline {
        operation: Operation::PointLookup,
        engine: "frankensqlite".to_owned(),
        row_count: 200,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 3. Range scan.
    let (lat, thr) = measure_operation(warmup, iters, || {
        let rows = conn
            .query("SELECT * FROM bench WHERE id >= 50 AND id < 100")
            .unwrap();
        assert_eq!(rows.len(), 50);
    });
    report.baselines.push(OperationBaseline {
        operation: Operation::RangeScan,
        engine: "frankensqlite".to_owned(),
        row_count: 200,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 4. Single-row insert (into a separate disposable table per measurement).
    let conn4 = fsqlite::Connection::open(":memory:").unwrap();
    conn4
        .execute("CREATE TABLE ins_test (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    let mut insert_id = 1_i64;
    let (lat, thr) = measure_operation(warmup, iters, || {
        conn4
            .execute(&format!(
                "INSERT INTO ins_test VALUES ({insert_id}, 'val_{insert_id}')"
            ))
            .unwrap();
        insert_id += 1;
    });
    report.baselines.push(OperationBaseline {
        operation: Operation::SingleRowInsert,
        engine: "frankensqlite".to_owned(),
        row_count: 0,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 5. Batch insert.
    let (lat, thr) = measure_operation(warmup, iters, || {
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
    report.baselines.push(OperationBaseline {
        operation: Operation::BatchInsert,
        engine: "frankensqlite".to_owned(),
        row_count: 100,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 6. Single-row update.
    let mut upd_id = 1_i64;
    let (lat, thr) = measure_operation(warmup, iters, || {
        conn.execute(&format!(
            "UPDATE bench SET score = {} WHERE id = {upd_id}",
            upd_id * 13,
        ))
        .unwrap();
        upd_id = (upd_id % 200) + 1;
    });
    report.baselines.push(OperationBaseline {
        operation: Operation::SingleRowUpdate,
        engine: "frankensqlite".to_owned(),
        row_count: 200,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 7. Single-row delete (use a disposable table).
    let conn7 = fsqlite::Connection::open(":memory:").unwrap();
    conn7
        .execute("CREATE TABLE del_test (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    for j in 1..=1000_i64 {
        conn7
            .execute(&format!("INSERT INTO del_test VALUES ({j}, 'v{j}')"))
            .unwrap();
    }
    let mut del_id = 1_i64;
    let (lat, thr) = measure_operation(warmup, iters, || {
        conn7
            .execute(&format!("DELETE FROM del_test WHERE id = {del_id}"))
            .unwrap();
        del_id += 1;
    });
    report.baselines.push(OperationBaseline {
        operation: Operation::SingleRowDelete,
        engine: "frankensqlite".to_owned(),
        row_count: 1000,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 8. 2-way equi-join.
    let (lat, thr) = measure_operation(warmup, iters, || {
        let rows = conn
            .query(
                "SELECT bench.id, bench.name, bench2.label \
                 FROM bench INNER JOIN bench2 ON bench.id = bench2.bench_id",
            )
            .unwrap();
        assert!(!rows.is_empty());
    });
    report.baselines.push(OperationBaseline {
        operation: Operation::TwoWayEquiJoin,
        engine: "frankensqlite".to_owned(),
        row_count: 200,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // 9. Aggregation.
    let (lat, thr) = measure_operation(warmup, iters, || {
        let rows = conn
            .query("SELECT COUNT(*), SUM(score), AVG(score) FROM bench")
            .unwrap();
        assert_eq!(rows.len(), 1);
    });
    report.baselines.push(OperationBaseline {
        operation: Operation::Aggregation,
        engine: "frankensqlite".to_owned(),
        row_count: 200,
        iterations: iters,
        warmup_iterations: warmup,
        latency: lat,
        throughput_ops_per_sec: thr,
    });

    // Verify we captured all 9.
    assert_eq!(report.baselines.len(), 9);

    // Verify JSON roundtrip.
    let json = report.to_pretty_json().unwrap();
    let parsed = BaselineReport::from_json(&json).unwrap();
    assert_eq!(parsed.baselines.len(), 9);

    // Verify no regressions against self (should all be exact match).
    let results = report.check_regression(&parsed, DEFAULT_REGRESSION_THRESHOLD);
    for r in &results {
        assert!(
            !r.regressed,
            "self-comparison should not regress: {}",
            r.summary()
        );
    }
}
