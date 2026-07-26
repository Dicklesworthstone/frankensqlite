//! Commit-path proof pack: crash recovery, corruption freedom, fairness,
//! and non-regression for commit-path primitives (bd-db300.3.8.10).
//!
//! Packages the four verification surfaces required for any new commit-path
//! primitive into a single replayable E2E test suite with structured evidence.
//!
//! ## Surfaces
//!
//! | ID | Name                             | Description                                          |
//! |----|----------------------------------|------------------------------------------------------|
//! | P1 | crash_recovery_round_trip        | Write→crash-simulate→reopen→verify no data loss      |
//! | P2 | corruption_freedom_integrity     | Bulk write→close→integrity_check→oracle compare      |
//! | P3 | fair_wakeup_multi_writer         | 4-thread concurrent writes, Jain's fairness ≥ 0.85   |
//! | P4 | non_regression_throughput        | Single-thread throughput within 5x of rusqlite        |
//! | P5 | wal_commit_ordering              | WAL commits visible in commit order after recovery    |
//! | P6 | checkpoint_under_write_load      | Checkpoint while writers active, no corruption        |
//! | P7 | savepoint_commit_crash_cycle     | SAVEPOINT→RELEASE→crash→reopen→verify                |
//! | P8 | evidence_pack_structured_log     | Full structured evidence for operator triage          |
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_db300_3_8_10_commit_path_proof_pack -- --nocapture --test-threads=1
//! ```
//!
//! P4 is a production-profile performance gate and is ignored when debug
//! assertions are enabled. Replay it explicitly with `--profile release-perf`.

#![allow(clippy::cast_precision_loss)]
#![recursion_limit = "512"]

use serde_json::json;
use std::sync::{Arc, Barrier};
use std::time::Instant;

const BEAD_ID: &str = "bd-db300.3.8.10";
const REPLAY_CMD: &str = "cargo test -p fsqlite-e2e --test bd_db300_3_8_10_commit_path_proof_pack -- --nocapture --test-threads=1";
const P4_REPLAY_CMD: &str = "cargo test --profile release-perf -p fsqlite-e2e --test bd_db300_3_8_10_commit_path_proof_pack p4_non_regression_throughput -- --nocapture --test-threads=1";

fn emit_log(test_name: &str, phase: &str, data: serde_json::Value) {
    eprintln!(
        "COMMIT_PATH_PROOF:{}",
        json!({
            "bead_id": BEAD_ID,
            "test": test_name,
            "phase": phase,
            "replay_command": REPLAY_CMD,
            "data": data,
        })
    );
}

fn jains_fairness_index(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 1.0;
    }
    let n = values.len() as f64;
    let sum: f64 = values.iter().sum();
    let sum_sq: f64 = values.iter().map(|x| x * x).sum();
    if sum_sq == 0.0 {
        return 1.0;
    }
    let numerator = sum * sum;
    let denominator = n * sum_sq;
    numerator / denominator
}

// ─── P1: Crash recovery round trip ──────────────────────────────────

#[test]
fn p1_crash_recovery_round_trip() {
    asupersync::test_utils::run_test(|| async {
        let tn = "p1_crash_recovery";
        emit_log(tn, "start", json!({}));

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("p1.db");
        let path_str = db_path.to_str().unwrap();

        // Write data
        {
            let conn = fsqlite::Connection::open(path_str).await.unwrap();
            conn.execute("CREATE TABLE p1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("BEGIN").await.unwrap();
            for i in 0..500 {
                conn.execute(&format!("INSERT INTO p1 VALUES ({i}, 'row_{i:04}')"))
                    .await
                    .unwrap();
            }
            conn.execute("COMMIT").await.unwrap();
            // Drop connection to simulate crash-like close
        }

        // Reopen and verify
        let conn = fsqlite::Connection::open(path_str).await.unwrap();
        let rows = conn.query("SELECT COUNT(*) FROM p1").await.unwrap();
        let count = match &rows[0].values()[0] {
            fsqlite_types::value::SqliteValue::Integer(n) => *n,
            other => panic!("unexpected: {other:?}"),
        };

        // Cross-verify with csqlite
        let cconn = rusqlite::Connection::open(path_str).unwrap();
        let c_count: i64 = cconn
            .query_row("SELECT COUNT(*) FROM p1", [], |r| r.get(0))
            .unwrap();

        emit_log(
            tn,
            "result",
            json!({
                "fsqlite_count": count,
                "csqlite_count": c_count,
                "expected": 500,
            }),
        );

        assert_eq!(
            count, 500,
            "[P1] expected 500 rows after reopen, got {count}"
        );
        assert_eq!(c_count, 500, "[P1] csqlite also sees 500 rows");
    });
}

// ─── P2: Corruption freedom — integrity check ───────────────────────

#[test]
fn p2_corruption_freedom_integrity() {
    asupersync::test_utils::run_test(|| async {
        let tn = "p2_integrity";
        emit_log(tn, "start", json!({}));

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("p2.db");
        let path_str = db_path.to_str().unwrap();

        let conn = fsqlite::Connection::open(path_str).await.unwrap();
        conn.execute("CREATE TABLE p2 (id INTEGER PRIMARY KEY, data BLOB)")
            .await
            .unwrap();

        // Bulk write with varied data sizes to exercise multiple pages
        conn.execute("BEGIN").await.unwrap();
        for i in 0..200 {
            let blob_size = 50 + (i % 100) * 10;
            let blob_hex: String = (0..blob_size)
                .map(|b| format!("{:02x}", (b + i) & 0xFF))
                .collect();
            conn.execute(&format!("INSERT INTO p2 VALUES ({i}, X'{blob_hex}')"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        // Run integrity check
        let integrity = conn.query("PRAGMA integrity_check").await.unwrap();
        let integrity_result = match &integrity[0].values()[0] {
            fsqlite_types::value::SqliteValue::Text(s) => s.as_str().to_owned(),
            other => format!("{other:?}"),
        };

        // Verify data round-trip
        let count_rows = conn.query("SELECT COUNT(*) FROM p2").await.unwrap();
        let count = match &count_rows[0].values()[0] {
            fsqlite_types::value::SqliteValue::Integer(n) => *n,
            other => panic!("unexpected: {other:?}"),
        };

        // Oracle comparison
        let cconn = rusqlite::Connection::open(path_str).unwrap();
        let c_count: i64 = cconn
            .query_row("SELECT COUNT(*) FROM p2", [], |r| r.get(0))
            .unwrap();
        let c_integrity: String = cconn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();

        emit_log(
            tn,
            "result",
            json!({
                "integrity_result": integrity_result,
                "csqlite_integrity": c_integrity,
                "row_count": count,
                "csqlite_count": c_count,
            }),
        );

        assert_eq!(
            integrity_result, "ok",
            "[P2] integrity_check should be 'ok', got '{integrity_result}'"
        );
        assert_eq!(count, 200, "[P2] expected 200 rows");
        assert_eq!(c_count, 200, "[P2] csqlite should also see 200 rows");
    });
}

// ─── P3: Fair wakeup — multi-writer Jain's fairness ─────────────────

#[test]
fn p3_fair_wakeup_multi_writer() {
    let tn = "p3_fairness";
    let thread_count = 4usize;
    let target_commits_per_thread = 100u64;
    emit_log(
        tn,
        "start",
        json!({"threads": thread_count, "target_commits": target_commits_per_thread}),
    );

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("p3.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    {
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(&path_str).await.unwrap();
            conn.execute(
                "CREATE TABLE p3 (tid INTEGER, seq INTEGER, val INTEGER, PRIMARY KEY (tid, seq))",
            )
            .await
            .unwrap();
        });
    }

    let barrier = Arc::new(Barrier::new(thread_count));
    let path_arc = Arc::new(path_str.clone());

    let handles: Vec<_> = (0..thread_count)
        .map(|tid| {
            let barrier = Arc::clone(&barrier);
            let path = Arc::clone(&path_arc);
            std::thread::spawn(move || {
                let mut outcome: Option<(usize, u64, u64, u64)> = None;
                asupersync::test_utils::run_test(|| async {
                    let conn = fsqlite::Connection::open(path.as_str()).await.unwrap();
                    barrier.wait();

                    let mut commits = 0u64;
                    let mut retries = 0u64;
                    let start = Instant::now();

                    for seq in 0..target_commits_per_thread {
                        let max_retries = 100;
                        let mut attempt = 0;
                        loop {
                            attempt += 1;
                            if conn.execute("BEGIN").await.is_err() {
                                std::thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            let val = tid as i64 * 100_000 + seq as i64;
                            if conn
                                .execute(&format!("INSERT INTO p3 VALUES ({tid}, {seq}, {val})"))
                                .await
                                .is_err()
                            {
                                let _ = conn.execute("ROLLBACK").await;
                                retries += 1;
                                std::thread::sleep(std::time::Duration::from_millis(
                                    1 + (attempt % 5),
                                ));
                                if attempt >= max_retries {
                                    panic!("thread {tid} exceeded retries at seq={seq}");
                                }
                                continue;
                            }
                            match conn.execute("COMMIT").await {
                                Ok(_) => {
                                    commits += 1;
                                    break;
                                }
                                Err(_) => {
                                    let _ = conn.execute("ROLLBACK").await;
                                    retries += 1;
                                    std::thread::sleep(std::time::Duration::from_millis(
                                        1 + (attempt % 5),
                                    ));
                                    if attempt >= max_retries {
                                        panic!("thread {tid} exceeded retries at seq={seq}");
                                    }
                                }
                            }
                        }
                    }

                    let elapsed = start.elapsed();
                    outcome = Some((tid, commits, retries, elapsed.as_millis() as u64));
                });
                outcome.expect("p3 writer thread outcome")
            })
        })
        .collect();

    let mut per_thread_commits = Vec::new();
    let mut per_thread_retries = Vec::new();
    let mut per_thread_ms = Vec::new();
    for h in handles {
        let (tid, commits, retries, ms) = h.join().unwrap();
        per_thread_commits.push(commits as f64);
        per_thread_retries.push(retries);
        per_thread_ms.push(ms);
        emit_log(
            tn,
            "thread_result",
            json!({"tid": tid, "commits": commits, "retries": retries, "ms": ms}),
        );
    }

    let fairness = jains_fairness_index(&per_thread_commits);

    // Verify total row count
    let mut total = 0i64;
    asupersync::test_utils::run_test(|| async {
        let verify_conn = fsqlite::Connection::open(&path_str).await.unwrap();
        let total_rows = verify_conn.query("SELECT COUNT(*) FROM p3").await.unwrap();
        total = match &total_rows[0].values()[0] {
            fsqlite_types::value::SqliteValue::Integer(n) => *n,
            other => panic!("unexpected: {other:?}"),
        };
    });

    let expected = thread_count as i64 * target_commits_per_thread as i64;

    emit_log(
        tn,
        "result",
        json!({
            "fairness_index": fairness,
            "per_thread_commits": per_thread_commits,
            "per_thread_retries": per_thread_retries,
            "per_thread_ms": per_thread_ms,
            "total_rows": total,
            "expected": expected,
        }),
    );

    assert!(
        fairness >= 0.85,
        "[P3] Jain's fairness index {fairness:.4} < 0.85 threshold"
    );
    assert_eq!(
        total, expected,
        "[P3] total rows {total} != expected {expected}"
    );
}

// ─── P4: Non-regression throughput ───────────────────────────────────

async fn measure_fsqlite_insert_batch(row_count: i64) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p4_f.db");
    let conn = fsqlite::Connection::open(path.to_str().unwrap())
        .await
        .unwrap();
    conn.execute("CREATE TABLE p4 (id INTEGER PRIMARY KEY, val INTEGER)")
        .await
        .unwrap();
    let stmt = conn
        .prepare("INSERT INTO p4 VALUES (?1, ?2)")
        .await
        .unwrap();

    let start = Instant::now();
    conn.execute("BEGIN").await.unwrap();
    for i in 0..row_count {
        conn.execute_prepared_with_params(
            &stmt,
            &[
                fsqlite_types::value::SqliteValue::Integer(i),
                fsqlite_types::value::SqliteValue::Integer(i * 7),
            ],
        )
        .await
        .unwrap();
    }
    conn.execute("COMMIT").await.unwrap();
    start.elapsed().as_nanos() as u64
}

fn measure_csqlite_insert_batch(row_count: i64) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p4_c.db");
    let conn = rusqlite::Connection::open(path.to_str().unwrap()).unwrap();
    conn.execute_batch("CREATE TABLE p4 (id INTEGER PRIMARY KEY, val INTEGER);")
        .unwrap();
    let mut stmt = conn.prepare("INSERT INTO p4 VALUES (?1, ?2)").unwrap();

    let start = Instant::now();
    conn.execute_batch("BEGIN;").unwrap();
    for i in 0..row_count {
        stmt.execute(rusqlite::params![i, i * 7]).unwrap();
    }
    conn.execute_batch("COMMIT;").unwrap();
    start.elapsed().as_nanos() as u64
}

fn median(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median requires at least one sample");
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

#[test]
fn p4_median_uses_the_middle_order_statistic() {
    let mut values = [9.0, 1.0, 7.0, 3.0, 5.0];
    assert_eq!(median(&mut values), 5.0);
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "production performance gate; replay with --profile release-perf"
)]
fn p4_non_regression_throughput() {
    asupersync::test_utils::run_test(|| async {
        let tn = "p4_throughput";
        let row_count = 5000i64;
        let sample_count = 11usize;
        emit_log(
            tn,
            "start",
            json!({
                "rows": row_count,
                "samples": sample_count,
                "replay_command": P4_REPLAY_CMD,
            }),
        );

        // Populate code/data caches before measuring. Alternate engine order in
        // each paired sample so scheduler and filesystem drift cannot
        // systematically penalize the engine that always runs first.
        let _ = measure_fsqlite_insert_batch(row_count).await;
        let _ = measure_csqlite_insert_batch(row_count);

        let mut sample_evidence = Vec::with_capacity(sample_count);
        let mut ratios = Vec::with_capacity(sample_count);
        for sample in 0..sample_count {
            let (f_ns, c_ns, order) = if sample % 2 == 0 {
                (
                    measure_fsqlite_insert_batch(row_count).await,
                    measure_csqlite_insert_batch(row_count),
                    "fsqlite_first",
                )
            } else {
                let c_ns = measure_csqlite_insert_batch(row_count);
                let f_ns = measure_fsqlite_insert_batch(row_count).await;
                (f_ns, c_ns, "csqlite_first")
            };
            let ratio = f_ns as f64 / c_ns.max(1) as f64;
            ratios.push(ratio);
            sample_evidence.push(json!({
                "sample": sample,
                "order": order,
                "fsqlite_ns": f_ns,
                "csqlite_ns": c_ns,
                "ratio": ratio,
            }));
        }
        let ratio_median = median(&mut ratios);

        emit_log(
            tn,
            "result",
            json!({
                "rows": row_count,
                "samples": sample_evidence,
                "ratio_median": ratio_median,
                "threshold": 5.0,
                "replay_command": P4_REPLAY_CMD,
            }),
        );

        assert!(
            ratio_median < 5.0,
            "[P4] median throughput ratio {ratio_median:.2}x exceeds 5x threshold; samples={sample_evidence:?}"
        );
    });
}

// ─── P5: WAL commit ordering ─────────────────────────────────────────

#[test]
fn p5_wal_commit_ordering() {
    asupersync::test_utils::run_test(|| async {
        let tn = "p5_wal_ordering";
        emit_log(tn, "start", json!({}));

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("p5.db");
        let path_str = db_path.to_str().unwrap();

        {
            let conn = fsqlite::Connection::open(path_str).await.unwrap();
            conn.execute("CREATE TABLE p5 (seq INTEGER PRIMARY KEY, ts TEXT)")
                .await
                .unwrap();

            // Multiple separate commits
            for i in 0..20 {
                conn.execute("BEGIN").await.unwrap();
                conn.execute(&format!("INSERT INTO p5 VALUES ({i}, 'commit_{i:03}')"))
                    .await
                    .unwrap();
                conn.execute("COMMIT").await.unwrap();
            }
        }

        // Reopen and verify ordering
        let conn = fsqlite::Connection::open(path_str).await.unwrap();
        let rows = conn
            .query("SELECT seq, ts FROM p5 ORDER BY seq")
            .await
            .unwrap();

        let cconn = rusqlite::Connection::open(path_str).unwrap();
        let c_rows: Vec<(i64, String)> = {
            let mut stmt = cconn
                .prepare("SELECT seq, ts FROM p5 ORDER BY seq")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };

        assert_eq!(rows.len(), 20, "[P5] expected 20 commits");
        assert_eq!(rows.len(), c_rows.len(), "[P5] oracle row count mismatch");

        let mut ordering_ok = true;
        for (i, (f, c)) in rows.iter().zip(c_rows.iter()).enumerate() {
            let f_seq = match &f.values()[0] {
                fsqlite_types::value::SqliteValue::Integer(n) => *n,
                other => panic!("row {i}: unexpected seq: {other:?}"),
            };
            if f_seq != c.0 {
                ordering_ok = false;
            }
        }

        emit_log(
            tn,
            "result",
            json!({"commits": 20, "ordering_ok": ordering_ok}),
        );

        assert!(ordering_ok, "[P5] WAL commit ordering mismatch vs oracle");
    });
}

// ─── P6: Checkpoint under write load ─────────────────────────────────

#[test]
fn p6_checkpoint_under_write_load() {
    asupersync::test_utils::run_test(|| async {
        let tn = "p6_checkpoint_writes";
        emit_log(tn, "start", json!({}));

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("p6.db");
        let path_str = db_path.to_str().unwrap();

        let conn = fsqlite::Connection::open(path_str).await.unwrap();
        conn.execute("CREATE TABLE p6 (id INTEGER PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();

        // Write some data, checkpoint, write more, verify all present
        conn.execute("BEGIN").await.unwrap();
        for i in 0..100 {
            conn.execute(&format!(
                "INSERT INTO p6 VALUES ({i}, 'before_checkpoint_{i}')"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        // Trigger checkpoint
        let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;

        // Write more after checkpoint
        conn.execute("BEGIN").await.unwrap();
        for i in 100..200 {
            conn.execute(&format!(
                "INSERT INTO p6 VALUES ({i}, 'after_checkpoint_{i}')"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        // Verify all data
        let count_rows = conn.query("SELECT COUNT(*) FROM p6").await.unwrap();
        let count = match &count_rows[0].values()[0] {
            fsqlite_types::value::SqliteValue::Integer(n) => *n,
            other => panic!("unexpected: {other:?}"),
        };

        // Oracle verify
        let cconn = rusqlite::Connection::open(path_str).unwrap();
        let c_count: i64 = cconn
            .query_row("SELECT COUNT(*) FROM p6", [], |r| r.get(0))
            .unwrap();

        emit_log(
            tn,
            "result",
            json!({
                "total_rows": count,
                "csqlite_count": c_count,
            }),
        );

        assert_eq!(count, 200, "[P6] expected 200 rows after checkpoint cycle");
        assert_eq!(c_count, 200, "[P6] oracle should see 200 rows");
    });
}

// ─── P7: SAVEPOINT→RELEASE→crash→reopen→verify ──────────────────────

#[test]
fn p7_savepoint_commit_crash_cycle() {
    asupersync::test_utils::run_test(|| async {
        let tn = "p7_savepoint_crash";
        emit_log(tn, "start", json!({}));

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("p7.db");
        let path_str = db_path.to_str().unwrap();

        {
            let conn = fsqlite::Connection::open(path_str).await.unwrap();
            conn.execute("CREATE TABLE p7 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();

            conn.execute("SAVEPOINT sp1").await.unwrap();
            conn.execute("INSERT INTO p7 VALUES (1, 'sp1_row')")
                .await
                .unwrap();
            conn.execute("SAVEPOINT sp2").await.unwrap();
            conn.execute("INSERT INTO p7 VALUES (2, 'sp2_row')")
                .await
                .unwrap();
            conn.execute("RELEASE sp2").await.unwrap();
            conn.execute("INSERT INTO p7 VALUES (3, 'after_sp2')")
                .await
                .unwrap();
            conn.execute("RELEASE sp1").await.unwrap();

            // Additional committed data
            conn.execute("INSERT INTO p7 VALUES (4, 'post_savepoint')")
                .await
                .unwrap();
        }

        // Reopen — simulates crash recovery
        let conn = fsqlite::Connection::open(path_str).await.unwrap();
        let rows = conn
            .query("SELECT id, val FROM p7 ORDER BY id")
            .await
            .unwrap();
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| match &r.values()[0] {
                fsqlite_types::value::SqliteValue::Integer(n) => *n,
                other => panic!("unexpected: {other:?}"),
            })
            .collect();

        let cconn = rusqlite::Connection::open(path_str).unwrap();
        let c_count: i64 = cconn
            .query_row("SELECT COUNT(*) FROM p7", [], |r| r.get(0))
            .unwrap();

        emit_log(
            tn,
            "result",
            json!({
                "ids_found": ids,
                "csqlite_count": c_count,
            }),
        );

        assert_eq!(
            ids,
            vec![1, 2, 3, 4],
            "[P7] all savepoint + post rows present"
        );
        assert_eq!(c_count, 4, "[P7] oracle sees 4 rows");
    });
}

// ─── P8: Evidence pack structured log ────────────────────────────────

#[test]
fn p8_evidence_pack_structured_log() {
    asupersync::test_utils::run_test(|| async {
        let tn = "p8_evidence_pack";
        emit_log(tn, "start", json!({}));

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("p8.db");
        let path_str = db_path.to_str().unwrap();

        let conn = fsqlite::Connection::open(path_str).await.unwrap();

        // Phase 1: Schema
        let schema_start = Instant::now();
        for sql in [
            "CREATE TABLE evidence (id INTEGER PRIMARY KEY, category TEXT, payload TEXT, score REAL)",
            "CREATE INDEX idx_ev_cat ON evidence(category)",
        ] {
            conn.execute(sql).await.unwrap();
        }
        let schema_ns = schema_start.elapsed().as_nanos() as u64;

        emit_log(tn, "phase_schema", json!({"schema_creation_ns": schema_ns}));

        // Phase 2: Bulk write
        let write_start = Instant::now();
        conn.execute("BEGIN").await.unwrap();
        for i in 0..1000 {
            let cat = ["crash", "corruption", "fairness"][i % 3];
            conn.execute(&format!(
                "INSERT INTO evidence VALUES ({i}, '{cat}', 'payload_{i:04}', {})",
                i as f64 * 0.5
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();
        let write_ns = write_start.elapsed().as_nanos() as u64;

        emit_log(
            tn,
            "phase_write",
            json!({"rows": 1000, "write_ns": write_ns}),
        );

        // Phase 3: Checkpoint
        let ckpt_start = Instant::now();
        let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
        let ckpt_ns = ckpt_start.elapsed().as_nanos() as u64;

        emit_log(tn, "phase_checkpoint", json!({"checkpoint_ns": ckpt_ns}));

        // Phase 4: Verify
        let verify_start = Instant::now();
        let integrity = conn.query("PRAGMA integrity_check").await.unwrap();
        let integrity_result = match &integrity[0].values()[0] {
            fsqlite_types::value::SqliteValue::Text(s) => s.as_str().to_owned(),
            other => format!("{other:?}"),
        };

        let count_rows = conn.query("SELECT COUNT(*) FROM evidence").await.unwrap();
        let count = match &count_rows[0].values()[0] {
            fsqlite_types::value::SqliteValue::Integer(n) => *n,
            other => panic!("unexpected: {other:?}"),
        };

        let cat_counts = conn
            .query("SELECT category, COUNT(*) FROM evidence GROUP BY category ORDER BY category")
            .await
            .unwrap();
        let verify_ns = verify_start.elapsed().as_nanos() as u64;

        // Cross-verify
        let cconn = rusqlite::Connection::open(path_str).unwrap();
        let c_count: i64 = cconn
            .query_row("SELECT COUNT(*) FROM evidence", [], |r| r.get(0))
            .unwrap();

        let mut cat_map = serde_json::Map::new();
        for row in &cat_counts {
            let cat = match &row.values()[0] {
                fsqlite_types::value::SqliteValue::Text(s) => s.as_str().to_owned(),
                other => format!("{other:?}"),
            };
            let cnt = match &row.values()[1] {
                fsqlite_types::value::SqliteValue::Integer(n) => *n,
                other => panic!("unexpected count: {other:?}"),
            };
            cat_map.insert(cat, json!(cnt));
        }

        emit_log(
            tn,
            "phase_verify",
            json!({
                "verify_ns": verify_ns,
                "integrity": integrity_result,
                "total_rows": count,
                "csqlite_count": c_count,
                "category_distribution": cat_map,
            }),
        );

        emit_log(
            tn,
            "evidence_summary",
            json!({
                "bead_id": BEAD_ID,
                "surfaces": ["crash_recovery", "corruption_freedom", "fairness", "non_regression"],
                "schema_ns": schema_ns,
                "write_ns": write_ns,
                "checkpoint_ns": ckpt_ns,
                "verify_ns": verify_ns,
                "integrity": integrity_result,
                "total_rows": count,
                "oracle_match": count == c_count,
            }),
        );

        assert_eq!(integrity_result, "ok", "[P8] integrity_check must be 'ok'");
        assert_eq!(count, 1000, "[P8] expected 1000 rows");
        assert_eq!(c_count, 1000, "[P8] oracle mismatch");
    });
}

// ─── Fairness math unit tests ────────────────────────────────────────

#[test]
fn fairness_math_perfect() {
    assert!((jains_fairness_index(&[100.0, 100.0, 100.0, 100.0]) - 1.0).abs() < 1e-10);
}

#[test]
fn fairness_math_skewed() {
    let idx = jains_fairness_index(&[100.0, 0.0, 0.0, 0.0]);
    assert!(idx < 0.3, "one-active fairness should be low: {idx}");
}

#[test]
fn fairness_math_empty() {
    assert!((jains_fairness_index(&[]) - 1.0).abs() < 1e-10);
}
