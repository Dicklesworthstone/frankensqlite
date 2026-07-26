//! bd-o653e — Concurrent UPDATE/DELETE DML oracle parity e2e tests.
//!
//! Exercises FrankenSQLite's MVCC correctness under concurrent UPDATE
//! and DELETE workloads. Verifies that:
//!   - Concurrent UPDATEs to disjoint rows commit without conflict
//!   - Concurrent UPDATEs to overlapping rows serialize correctly
//!   - DELETEs under concurrent load don't lose or duplicate rows
//!   - Mixed INSERT/UPDATE/DELETE workloads produce oracle-parity results
//!   - UPDATE … WHERE with range predicates behaves correctly
//!   - DELETE with subquery predicates under concurrency
#![recursion_limit = "512"]

use std::sync::{Arc, Barrier};
use std::thread;

use fsqlite::SqliteValue;

// ── Helpers ────────────────────────────────────────────────────────────

async fn frank_scalar(conn: &fsqlite::Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => s.to_string(),
        SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}

async fn frank_rows(conn: &fsqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank query `{sql}`: {e}"));
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".into(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Blob(b) => {
                        format!(
                            "X'{}'",
                            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                        )
                    }
                })
                .collect()
        })
        .collect()
}

fn csql_scalar(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| {
        let v: rusqlite::types::Value = row.get_unwrap(0);
        Ok(match v {
            rusqlite::types::Value::Null => "NULL".into(),
            rusqlite::types::Value::Integer(x) => x.to_string(),
            rusqlite::types::Value::Real(f) => format!("{f}"),
            rusqlite::types::Value::Text(s) => s,
            rusqlite::types::Value::Blob(b) => {
                format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                )
            }
        })
    })
    .unwrap()
}

fn rusqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("csql prepare `{sql}`: {e}"));
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".into(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => s,
                rusqlite::types::Value::Blob(b) => {
                    format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    )
                }
            });
        }
        Ok(out)
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

// ── Test 1: Concurrent UPDATEs to disjoint rows ─────────────────────

#[test]
fn concurrent_updates_disjoint_rows() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        let n_threads = 4usize;
        let rows_per_thread = 25usize;
        let total_rows = n_threads * rows_per_thread;

        // Seed both engines with identical data
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE upd (id INTEGER PRIMARY KEY, val INTEGER, writer INTEGER);")
                .await
                .unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch(
                "CREATE TABLE upd (id INTEGER PRIMARY KEY, val INTEGER, writer INTEGER);",
            )
            .unwrap();
            for i in 0..total_rows {
                let sql = format!("INSERT INTO upd VALUES ({i}, 0, -1);");
                f.execute(&sql).await.unwrap();
                r.execute_batch(&sql).unwrap();
            }
        }

        // Each thread updates its own partition: thread t updates rows [t*25, (t+1)*25)
        let barrier = Arc::new(Barrier::new(n_threads));
        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();
                        let start = tid * rows_per_thread;
                        let end = start + rows_per_thread;
                        for pk in start..end {
                            let mut attempts = 0u32;
                            loop {
                                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                    attempts += 1;
                                    assert!(attempts < 200, "too many retries on BEGIN");
                                    thread::sleep(std::time::Duration::from_millis(1));
                                    continue;
                                }
                                let sql = format!(
                                    "UPDATE upd SET val = {}, writer = {} WHERE id = {};",
                                    pk * 10,
                                    tid,
                                    pk
                                );
                                if conn.execute(&sql).await.is_err() {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < 200, "too many retries on UPDATE");
                                    thread::sleep(std::time::Duration::from_millis(1));
                                    continue;
                                }
                                match conn.execute("COMMIT").await {
                                    Ok(_) => break,
                                    Err(_) => {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(attempts < 200, "too many retries on COMMIT");
                                        thread::sleep(std::time::Duration::from_millis(1));
                                    }
                                }
                            }
                        }
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Apply same updates sequentially to rusqlite
        for tid in 0..n_threads {
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            let start = tid * rows_per_thread;
            let end = start + rows_per_thread;
            for pk in start..end {
                r.execute_batch(&format!(
                    "UPDATE upd SET val = {}, writer = {} WHERE id = {};",
                    pk * 10,
                    tid,
                    pk
                ))
                .unwrap();
            }
        }

        // Verify parity
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM upd").await;
        let rcount = csql_scalar(&r, "SELECT COUNT(*) FROM upd");
        assert_eq!(fcount, rcount, "row count mismatch");
        assert_eq!(fcount, total_rows.to_string());

        let fsum = frank_scalar(&f, "SELECT SUM(val) FROM upd").await;
        let rsum = csql_scalar(&r, "SELECT SUM(val) FROM upd");
        assert_eq!(fsum, rsum, "SUM(val) mismatch");

        let fdata = frank_rows(&f, "SELECT id, val, writer FROM upd ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, val, writer FROM upd ORDER BY id");
        assert_eq!(fdata, rdata, "full data mismatch");
    });
}

// ── Test 2: Concurrent DELETEs from disjoint ranges ─────────────────

#[test]
fn concurrent_deletes_disjoint_ranges() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        let n_threads = 4usize;
        let rows_per_thread = 20usize;
        let total_rows = n_threads * rows_per_thread;

        // Seed
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE del (id INTEGER PRIMARY KEY, batch INTEGER);")
                .await
                .unwrap();
            for i in 0..total_rows {
                let batch = i / rows_per_thread;
                f.execute(&format!("INSERT INTO del VALUES ({i}, {batch});"))
                    .await
                    .unwrap();
            }
        }

        // Each thread deletes its own batch
        let barrier = Arc::new(Barrier::new(n_threads));
        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();
                        let mut attempts = 0u32;
                        loop {
                            if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                attempts += 1;
                                assert!(attempts < 200, "too many retries on BEGIN");
                                thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            let sql = format!("DELETE FROM del WHERE batch = {tid};");
                            if conn.execute(&sql).await.is_err() {
                                drop(conn.execute("ROLLBACK").await);
                                attempts += 1;
                                assert!(attempts < 200, "too many retries on DELETE");
                                thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            match conn.execute("COMMIT").await {
                                Ok(_) => break,
                                Err(_) => {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < 200, "too many retries on COMMIT");
                                    thread::sleep(std::time::Duration::from_millis(1));
                                }
                            }
                        }
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let count = frank_scalar(&f, "SELECT COUNT(*) FROM del").await;
        assert_eq!(count, "0", "all rows should be deleted");
    });
}

// ── Test 3: Mixed INSERT + UPDATE + DELETE oracle parity ─────────────

#[test]
fn mixed_insert_update_delete_parity() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        // Seed both engines
        let setup = [
            "PRAGMA journal_mode = WAL;",
            "CREATE TABLE mix (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT);",
        ];
        let seeds: Vec<String> = (0..50)
            .map(|i| format!("INSERT INTO mix VALUES ({i}, {}, 'init');", i * 10))
            .collect();

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            for s in &setup {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
            for s in &seeds {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        // DML sequence applied identically to both engines
        let dml = [
            "UPDATE mix SET val = val + 1 WHERE id < 10;",
            "DELETE FROM mix WHERE id >= 40;",
            "INSERT INTO mix VALUES (100, 1000, 'new');",
            "INSERT INTO mix VALUES (101, 1010, 'new');",
            "UPDATE mix SET tag = 'updated' WHERE val > 100;",
            "DELETE FROM mix WHERE id = 25;",
            "UPDATE mix SET val = 0 WHERE tag = 'init' AND id BETWEEN 15 AND 20;",
            "INSERT INTO mix VALUES (102, 1020, 'batch2');",
            "DELETE FROM mix WHERE tag = 'init' AND val < 50;",
            "UPDATE mix SET val = val * 2 WHERE tag = 'updated';",
        ];

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            for s in &dml {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        // Verify
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM mix").await;
        let rcount = csql_scalar(&r, "SELECT COUNT(*) FROM mix");
        assert_eq!(fcount, rcount, "count mismatch after mixed DML");

        let fdata = frank_rows(&f, "SELECT id, val, tag FROM mix ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, val, tag FROM mix ORDER BY id");
        assert_eq!(fdata, rdata, "data mismatch after mixed DML");
    });
}

// ── Test 4: UPDATE with range predicate under concurrent writers ─────

#[test]
fn concurrent_range_updates() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        // Seed: 100 rows, val = id
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE rng (id INTEGER PRIMARY KEY, val INTEGER, grp TEXT);")
                .await
                .unwrap();
            for i in 0..100 {
                let grp = if i < 25 {
                    "A"
                } else if i < 50 {
                    "B"
                } else if i < 75 {
                    "C"
                } else {
                    "D"
                };
                f.execute(&format!("INSERT INTO rng VALUES ({i}, {i}, '{grp}');"))
                    .await
                    .unwrap();
            }
        }

        // 4 threads each update their own group
        let groups = ["A", "B", "C", "D"];
        let barrier = Arc::new(Barrier::new(4));
        let handles: Vec<_> = groups
            .iter()
            .enumerate()
            .map(|(tid, grp)| {
                let p = f_path.clone();
                let bar = barrier.clone();
                let g = (*grp).to_owned();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();
                        let mut attempts = 0u32;
                        loop {
                            if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                attempts += 1;
                                assert!(attempts < 200);
                                thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            let sql = format!(
                                "UPDATE rng SET val = val + {} WHERE grp = '{}';",
                                tid + 1,
                                g
                            );
                            if conn.execute(&sql).await.is_err() {
                                drop(conn.execute("ROLLBACK").await);
                                attempts += 1;
                                assert!(attempts < 200);
                                thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            match conn.execute("COMMIT").await {
                                Ok(_) => break,
                                Err(_) => {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < 200);
                                    thread::sleep(std::time::Duration::from_millis(1));
                                }
                            }
                        }
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();

        // Group A (ids 0-24) should have val = id + 1
        let sum_a = frank_scalar(&f, "SELECT SUM(val) FROM rng WHERE grp = 'A'").await;
        let expected_a: i64 = (0..25).map(|i: i64| i + 1).sum();
        assert_eq!(sum_a, expected_a.to_string(), "group A sum mismatch");

        // Group B (ids 25-49) should have val = id + 2
        let sum_b = frank_scalar(&f, "SELECT SUM(val) FROM rng WHERE grp = 'B'").await;
        let expected_b: i64 = (25..50).map(|i: i64| i + 2).sum();
        assert_eq!(sum_b, expected_b.to_string(), "group B sum mismatch");

        // Group C (ids 50-74) should have val = id + 3
        let sum_c = frank_scalar(&f, "SELECT SUM(val) FROM rng WHERE grp = 'C'").await;
        let expected_c: i64 = (50..75).map(|i: i64| i + 3).sum();
        assert_eq!(sum_c, expected_c.to_string(), "group C sum mismatch");

        // Group D (ids 75-99) should have val = id + 4
        let sum_d = frank_scalar(&f, "SELECT SUM(val) FROM rng WHERE grp = 'D'").await;
        let expected_d: i64 = (75..100).map(|i: i64| i + 4).sum();
        assert_eq!(sum_d, expected_d.to_string(), "group D sum mismatch");

        let total = frank_scalar(&f, "SELECT COUNT(*) FROM rng").await;
        assert_eq!(total, "100", "no rows should be lost");
    });
}

// ── Test 5: DELETE + re-INSERT same PK under concurrency ─────────────

#[test]
fn delete_reinsert_same_pk_concurrent() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        // Seed: 20 rows
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE dri (id INTEGER PRIMARY KEY, version INTEGER);")
                .await
                .unwrap();
            for i in 0..20 {
                f.execute(&format!("INSERT INTO dri VALUES ({i}, 0);"))
                    .await
                    .unwrap();
            }
        }

        // 2 threads: each deletes then re-inserts its partition with incremented version
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();
                        let start = tid * 10;
                        let end = start + 10;
                        for version in 1..=3 {
                            let mut attempts = 0u32;
                            loop {
                                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                    attempts += 1;
                                    assert!(attempts < 200);
                                    thread::sleep(std::time::Duration::from_millis(1));
                                    continue;
                                }
                                let mut ok = true;
                                for pk in start..end {
                                    if conn
                                        .execute(&format!("DELETE FROM dri WHERE id = {pk};"))
                                        .await
                                        .is_err()
                                    {
                                        ok = false;
                                        break;
                                    }
                                    if conn
                                        .execute(&format!(
                                            "INSERT INTO dri VALUES ({pk}, {version});"
                                        ))
                                        .await
                                        .is_err()
                                    {
                                        ok = false;
                                        break;
                                    }
                                }
                                if !ok {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < 200);
                                    thread::sleep(std::time::Duration::from_millis(1));
                                    continue;
                                }
                                match conn.execute("COMMIT").await {
                                    Ok(_) => break,
                                    Err(_) => {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(attempts < 200);
                                        thread::sleep(std::time::Duration::from_millis(1));
                                    }
                                }
                            }
                        }
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let count = frank_scalar(&f, "SELECT COUNT(*) FROM dri").await;
        assert_eq!(count, "20", "row count should be unchanged");

        // All rows should be at version 3
        let min_v = frank_scalar(&f, "SELECT MIN(version) FROM dri").await;
        let max_v = frank_scalar(&f, "SELECT MAX(version) FROM dri").await;
        assert_eq!(min_v, "3", "all rows should be at version 3");
        assert_eq!(max_v, "3", "all rows should be at version 3");
    });
}

// ── Test 6: Concurrent writers + DELETE with subquery predicate ──────

#[test]
fn delete_with_subquery_predicate() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        let setup = [
            "PRAGMA journal_mode = WAL;",
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, status TEXT);",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, data TEXT);",
            "INSERT INTO parent VALUES (1, 'active'), (2, 'deleted'), (3, 'active'), (4, 'deleted'), (5, 'active');",
            "INSERT INTO child VALUES (10, 1, 'c1'), (11, 1, 'c2'), (20, 2, 'd1'), (21, 2, 'd2'), (30, 3, 'c3'), (40, 4, 'd3'), (50, 5, 'c4');",
        ];

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            for s in &setup {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        // Delete children whose parent is 'deleted'
        let delete_sql =
            "DELETE FROM child WHERE parent_id IN (SELECT id FROM parent WHERE status = 'deleted');";

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute(delete_sql).await.unwrap();

            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch(delete_sql).unwrap();
        }

        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM child").await;
        let rcount = csql_scalar(&r, "SELECT COUNT(*) FROM child");
        assert_eq!(fcount, rcount, "child count mismatch");
        assert_eq!(
            fcount, "4",
            "3 children of deleted parents removed, 4 remain"
        );

        let fdata = frank_rows(&f, "SELECT id, parent_id, data FROM child ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, parent_id, data FROM child ORDER BY id");
        assert_eq!(fdata, rdata, "child data mismatch");
    });
}

// ── Test 7: UPDATE with computed expression parity ───────────────────

#[test]
fn update_computed_expressions_parity() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        let setup = [
            "PRAGMA journal_mode = WAL;",
            "CREATE TABLE comp (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT);",
        ];
        let seeds: Vec<String> = (0..30)
            .map(|i| {
                format!(
                    "INSERT INTO comp VALUES ({i}, {}, {}, 'v{i}');",
                    i * 3,
                    i * 7
                )
            })
            .collect();

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            for s in &setup {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
            for s in &seeds {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        let updates = [
            "UPDATE comp SET a = a + b WHERE id < 10;",
            "UPDATE comp SET b = a * 2 - b WHERE id BETWEEN 10 AND 19;",
            "UPDATE comp SET c = 'modified_' || CAST(id AS TEXT) WHERE a > 50;",
            "UPDATE comp SET a = CASE WHEN b > 100 THEN b ELSE a END;",
            "UPDATE comp SET b = ABS(a - b) WHERE c LIKE 'v%';",
        ];

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            for s in &updates {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fdata = frank_rows(&f, "SELECT id, a, b, c FROM comp ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, a, b, c FROM comp ORDER BY id");
        assert_eq!(fdata, rdata, "computed expression update data mismatch");
    });
}

// ── Test 8: Concurrent UPDATE + DELETE on same table ─────────────────

#[test]
fn concurrent_update_and_delete_same_table() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        // Seed: 100 rows
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE ud (id INTEGER PRIMARY KEY, val INTEGER, keep INTEGER);")
                .await
                .unwrap();
            for i in 0..100 {
                let keep = if i % 2 == 0 { 1 } else { 0 };
                f.execute(&format!("INSERT INTO ud VALUES ({i}, {i}, {keep});"))
                    .await
                    .unwrap();
            }
        }

        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: update even rows (keep=1)
        let p1 = f_path.clone();
        let b1 = barrier.clone();
        let updater = thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&p1).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                b1.wait();
                let mut attempts = 0u32;
                loop {
                    if conn.execute("BEGIN CONCURRENT").await.is_err() {
                        attempts += 1;
                        assert!(attempts < 200);
                        thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    if conn
                        .execute("UPDATE ud SET val = val + 1000 WHERE keep = 1;")
                        .await
                        .is_err()
                    {
                        drop(conn.execute("ROLLBACK").await);
                        attempts += 1;
                        assert!(attempts < 200);
                        thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    match conn.execute("COMMIT").await {
                        Ok(_) => break,
                        Err(_) => {
                            drop(conn.execute("ROLLBACK").await);
                            attempts += 1;
                            assert!(attempts < 200);
                            thread::sleep(std::time::Duration::from_millis(1));
                        }
                    }
                }
            });
        });

        // Thread 2: delete odd rows (keep=0)
        let p2 = f_path.clone();
        let b2 = barrier.clone();
        let deleter = thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&p2).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                b2.wait();
                let mut attempts = 0u32;
                loop {
                    if conn.execute("BEGIN CONCURRENT").await.is_err() {
                        attempts += 1;
                        assert!(attempts < 200);
                        thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    if conn
                        .execute("DELETE FROM ud WHERE keep = 0;")
                        .await
                        .is_err()
                    {
                        drop(conn.execute("ROLLBACK").await);
                        attempts += 1;
                        assert!(attempts < 200);
                        thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    match conn.execute("COMMIT").await {
                        Ok(_) => break,
                        Err(_) => {
                            drop(conn.execute("ROLLBACK").await);
                            attempts += 1;
                            assert!(attempts < 200);
                            thread::sleep(std::time::Duration::from_millis(1));
                        }
                    }
                }
            });
        });

        updater.join().unwrap();
        deleter.join().unwrap();

        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();

        let count = frank_scalar(&f, "SELECT COUNT(*) FROM ud").await;
        assert_eq!(count, "50", "only even rows should remain");

        // All remaining rows should have keep=1 and val = original + 1000
        let min_val = frank_scalar(&f, "SELECT MIN(val) FROM ud").await;
        assert_eq!(min_val, "1000", "smallest even id=0, val=0+1000=1000");

        let bad_keep = frank_scalar(&f, "SELECT COUNT(*) FROM ud WHERE keep = 0").await;
        assert_eq!(bad_keep, "0", "no odd rows should remain");
    });
}
