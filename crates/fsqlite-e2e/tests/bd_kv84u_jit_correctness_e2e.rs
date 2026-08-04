//! bd-kv84u: Track N e2e tests — JIT compilation correctness,
//! interpreted vs JIT result parity across workloads.
//!
//! These tests run queries enough times to trigger JIT compilation
//! (typically ≥3 executions), then verify results match oracle.
//!
//! - J1: Repeated SELECT with arithmetic — results stable across iterations
//! - J2: Hot INSERT loop, verify data integrity after many repetitions
//! - J3: Hot UPDATE loop, verify cumulative correctness
//! - J4: Mixed DML hot loop (INSERT+UPDATE+SELECT)
//! - J5: Query with all column types under repeated execution
//! - J6: Aggregate query hot loop
//! - J7: Oracle parity under hot execution (rusqlite)
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

async fn get_int(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(sql).await.ok()?;
    let row = rows.first()?;
    match row.get(0)? {
        SqliteValue::Integer(v) => Some(*v),
        _ => None,
    }
}

// ─── J1: Repeated SELECT with arithmetic ──────────────────────────

#[test]
fn j1_repeated_select_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j1.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("create");
        conn.execute("BEGIN").await.expect("begin");
        for i in 1..=100 {
            conn.execute(&format!("INSERT INTO nums VALUES ({i}, {})", i * 7))
                .await
                .expect("insert");
        }
        conn.execute("COMMIT").await.expect("commit");

        // Execute same query 50 times — should trigger JIT after a few iterations
        let sql = "SELECT SUM(val * 2 + 1) FROM nums WHERE id <= 50";
        let mut results = Vec::new();
        for _ in 0..50 {
            let val = get_int(&conn, sql).await;
            results.push(val);
        }

        // All results must be identical
        let first = results[0];
        for (i, r) in results.iter().enumerate() {
            assert_eq!(
                *r, first,
                "J1: iteration {i} gave {r:?}, expected {first:?}"
            );
        }
        assert!(first.is_some(), "J1: query returned None");

        eprintln!("J1: 50 identical SELECT results — JIT parity OK, val={first:?}");
    });
}

// ─── J2: Hot INSERT loop ──────────────────────────────────────────

#[test]
fn j2_hot_insert_loop() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j2.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE hot_ins (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("create");

        // Many small transactions with same pattern
        for batch in 0..20 {
            conn.execute("BEGIN").await.expect("begin");
            for i in 0..50 {
                let id = batch * 50 + i + 1;
                conn.execute(&format!("INSERT INTO hot_ins VALUES ({id}, {})", id * 3))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let count = get_int(&conn, "SELECT COUNT(*) FROM hot_ins")
            .await
            .unwrap();
        assert_eq!(count, 1000, "J2: expected 1000 rows");

        let sum = get_int(&conn, "SELECT SUM(val) FROM hot_ins")
            .await
            .unwrap();
        // sum(i*3 for i in 1..=1000) = 3 * 1000*1001/2 = 1_501_500
        assert_eq!(sum, 1_501_500, "J2: sum wrong after hot insert loop");

        eprintln!("J2: 20 hot INSERT batches × 50 rows — data integrity OK");
    });
}

// ─── J3: Hot UPDATE loop ──────────────────────────────────────────

#[test]
fn j3_hot_update_loop() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j3.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("create");
        conn.execute("INSERT INTO counter VALUES (1, 0)")
            .await
            .expect("seed");

        // Run same UPDATE 100 times — enough to trigger JIT
        for _ in 0..100 {
            conn.execute("UPDATE counter SET val = val + 1 WHERE id = 1")
                .await
                .expect("update");
        }

        let val = get_int(&conn, "SELECT val FROM counter WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(val, 100, "J3: counter should be 100 after 100 increments");

        // More updates in transactions
        for _ in 0..50 {
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("UPDATE counter SET val = val + 2 WHERE id = 1")
                .await
                .expect("update");
            conn.execute("COMMIT").await.expect("commit");
        }

        let val2 = get_int(&conn, "SELECT val FROM counter WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(
            val2, 200,
            "J3: counter should be 200 after 50 more +2 updates"
        );

        eprintln!("J3: 150 hot UPDATEs — cumulative correctness verified");
    });
}

// ─── J4: Mixed DML hot loop ──────────────────────────────────────

#[test]
fn j4_mixed_dml_hot_loop() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j4.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE mixed (id INTEGER PRIMARY KEY, val INTEGER, round INTEGER)")
            .await
            .expect("create");

        for round in 0..30 {
            conn.execute("BEGIN").await.expect("begin");

            // INSERT 10 rows
            for i in 1..=10 {
                let id = round * 10 + i;
                conn.execute(&format!(
                    "INSERT OR REPLACE INTO mixed VALUES ({id}, {i}, {round})"
                ))
                .await
                .expect("insert");
            }

            // UPDATE based on round
            conn.execute(&format!(
                "UPDATE mixed SET val = val + {round} WHERE round = {round}"
            ))
            .await
            .expect("update");

            // DELETE old rounds (keep last 5)
            if round >= 5 {
                conn.execute(&format!("DELETE FROM mixed WHERE round < {}", round - 4))
                    .await
                    .expect("delete");
            }

            conn.execute("COMMIT").await.expect("commit");

            // Verify in each round
            let count = get_int(&conn, "SELECT COUNT(*) FROM mixed").await.unwrap();
            let expected_rounds = if round < 5 { round + 1 } else { 5 };
            let expected_rows = expected_rounds * 10;
            assert_eq!(
                count, expected_rows as i64,
                "J4: round {round} expected {expected_rows} rows, got {count}"
            );
        }

        eprintln!("J4: 30 mixed DML rounds — insert/update/delete hot loop correct");
    });
}

// ─── J5: All column types under repeated execution ────────────────

#[test]
fn j5_all_types_hot_query() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j5.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute(
            "CREATE TABLE typed (id INTEGER PRIMARY KEY, i_val INTEGER, r_val REAL, t_val TEXT, b_val BLOB)",
        )
        .await
        .expect("create");

        conn.execute("INSERT INTO typed VALUES (1, 42, 3.14, 'hello', X'DEADBEEF')")
            .await
            .expect("insert");
        conn.execute("INSERT INTO typed VALUES (2, NULL, NULL, NULL, NULL)")
            .await
            .expect("insert null");

        // Run the same query 30 times
        for iteration in 0..30 {
            let rows = conn
                .query("SELECT * FROM typed ORDER BY id")
                .await
                .expect("query");

            assert_eq!(rows.len(), 2, "J5: iter {iteration} row count wrong");

            // Check row 1 types
            match rows[0].get(1) {
                Some(SqliteValue::Integer(42)) => {}
                other => panic!("J5: iter {iteration} id=1 i_val wrong: {other:?}"),
            }
            match rows[0].get(3) {
                Some(SqliteValue::Text(s)) if s.as_str() == "hello" => {}
                other => panic!("J5: iter {iteration} id=1 t_val wrong: {other:?}"),
            }

            // Check row 2 NULLs
            match rows[1].get(1) {
                Some(SqliteValue::Null) | None => {}
                other => panic!("J5: iter {iteration} id=2 i_val should be NULL: {other:?}"),
            }
        }

        eprintln!("J5: 30 hot queries with all types — stable across iterations");
    });
}

// ─── J6: Aggregate hot loop ───────────────────────────────────────

#[test]
fn j6_aggregate_hot_loop() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j6.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE agg (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
            .await
            .expect("create");

        conn.execute("BEGIN").await.expect("begin");
        for i in 1..=500 {
            let grp = format!("g{}", i % 10);
            conn.execute(&format!("INSERT INTO agg VALUES ({i}, '{grp}', {i})"))
                .await
                .expect("insert");
        }
        conn.execute("COMMIT").await.expect("commit");

        // Run grouped aggregate 40 times
        let sql = "SELECT grp, COUNT(*), SUM(val) FROM agg GROUP BY grp ORDER BY grp";
        let mut first_result: Option<Vec<(i64, i64)>> = None;

        for iteration in 0..40 {
            let rows = conn.query(sql).await.expect("query");
            let current: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let cnt = match r.get(1) {
                        Some(SqliteValue::Integer(v)) => *v,
                        _ => -1,
                    };
                    let sum = match r.get(2) {
                        Some(SqliteValue::Integer(v)) => *v,
                        _ => -1,
                    };
                    (cnt, sum)
                })
                .collect();

            if let Some(ref first) = first_result {
                assert_eq!(
                    current, *first,
                    "J6: iteration {iteration} aggregate results differ from first"
                );
            } else {
                first_result = Some(current);
            }
        }

        eprintln!("J6: 40 grouped aggregate queries — stable results across iterations");
    });
}

// ─── J7: Oracle parity under hot execution ────────────────────────

#[test]
fn j7_oracle_hot_execution() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("j7_f.db");
        let c_path = dir.path().join("j7_c.db");

        let f = Connection::open(f_path.to_str().expect("path"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");

        let ddl = "CREATE TABLE hot (id INTEGER PRIMARY KEY, val INTEGER)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        // Seed both
        f.execute("BEGIN").await.expect("f begin");
        for i in 1..=200 {
            let sql = format!("INSERT INTO hot VALUES ({i}, {})", i * i);
            f.execute(&sql).await.expect("f insert");
            c.execute(&sql, []).expect("c insert");
        }
        f.execute("COMMIT").await.expect("f commit");

        // Run queries 30 times on both — fsqlite may JIT, csqlite won't
        let queries = [
            "SELECT SUM(val) FROM hot",
            "SELECT COUNT(*) FROM hot WHERE val > 1000",
            "SELECT MAX(val) - MIN(val) FROM hot",
            "SELECT AVG(val) FROM hot",
        ];

        for sql in &queries {
            let f_val = get_int(&f, sql).await;
            let c_val: Option<i64> = c
                .prepare(sql)
                .ok()
                .and_then(|mut stmt| stmt.query_row([], |r| r.get(0)).ok());

            // Run 30 times on fsqlite to trigger JIT
            for _ in 0..30 {
                let hot_val = get_int(&f, sql).await;
                assert_eq!(
                    hot_val, f_val,
                    "J7: hot execution changed result for: {sql}"
                );
            }

            // Compare with oracle
            assert_eq!(
                f_val, c_val,
                "J7: oracle mismatch for: {sql} — f={f_val:?}, c={c_val:?}"
            );
        }

        eprintln!("J7: 4 queries × 30 hot iterations — oracle parity maintained");
    });
}
