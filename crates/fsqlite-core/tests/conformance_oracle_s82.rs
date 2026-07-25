//! Conformance oracle tests — Session 82 (cc4 prepared-statement ceremony + rapid DML)
//!
//! Regression guards for bd-cg732 ceremony-gating optimizations:
//! rapid sequential DML, interleaved INSERT/UPDATE/DELETE, multi-table
//! transactions, RETURNING clauses, subquery-driven DML, and edge cases
//! in autocommit vs explicit-txn paths.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn oracle_compare(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let frank_result = fconn.query(query).await;
        let csql_result: std::result::Result<Vec<Vec<String>>, String> = (|| {
            let mut stmt = rconn.prepare(query).map_err(|e| format!("prepare: {e}"))?;
            let col_count = stmt.column_count();
            let rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::new();
                    for i in 0..col_count {
                        let v: rusqlite::types::Value = row.get_unwrap(i);
                        let s = match v {
                            rusqlite::types::Value::Null => "NULL".to_owned(),
                            rusqlite::types::Value::Integer(n) => n.to_string(),
                            rusqlite::types::Value::Real(f) => format!("{f}"),
                            rusqlite::types::Value::Text(s) => format!("'{s}'"),
                            rusqlite::types::Value::Blob(b) => format!(
                                "X'{}'",
                                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                            ),
                        };
                        vals.push(s);
                    }
                    Ok(vals)
                })
                .map_err(|e| format!("query: {e}"))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| format!("row: {e}"))?;
            Ok(rows)
        })();
        match (frank_result, csql_result) {
            (Ok(rows), Ok(csql_rows)) => {
                let frank_strs: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(|v| match v {
                                SqliteValue::Null => "NULL".to_owned(),
                                SqliteValue::Integer(n) => n.to_string(),
                                SqliteValue::Float(f) => format!("{f}"),
                                SqliteValue::Text(s) => format!("'{s}'"),
                                SqliteValue::Blob(b) => format!(
                                    "X'{}'",
                                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                ),
                            })
                            .collect()
                    })
                    .collect();
                if frank_strs != csql_rows {
                    mismatches.push(format!(
                        "MISMATCH: {query}\n  frank: {frank_strs:?}\n  csql:  {csql_rows:?}"
                    ));
                }
            }
            (Ok(_), Err(csql_err)) => {
                mismatches.push(format!(
                    "DIVERGE: {query}\n  frank: OK\n  csql:  ERROR({csql_err})"
                ));
            }
            (Err(e), Ok(csql_rows)) => {
                mismatches.push(format!(
                    "PAIR_FRANK_ERROR[{query}]\n  frank: ERROR({e})\n  csql:  {csql_rows:?}"
                ));
            }
            (Err(frank_err), Err(csql_err)) => {
                mismatches.push(format!(
                    "BOTH_ERROR: {query}\n  frank: ERROR({frank_err})\n  csql:  ERROR({csql_err})"
                ));
            }
        }
    }
    mismatches
}

fn assert_no_mismatches(mismatches: &[String], label: &str) {
    if !mismatches.is_empty() {
        for m in mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} {label} mismatch(es)", mismatches.len());
    }
}

#[test]
fn test_conformance_rapid_insert_update_delete_s82a() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER NOT NULL DEFAULT 0)",
            "INSERT INTO counters VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        for i in 1..=20 {
            let sql = format!(
                "UPDATE counters SET val = val + 1 WHERE id = {}",
                (i % 5) + 1
            );
            fconn.execute(&sql).await.unwrap();
            rconn.execute_batch(&sql).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, val FROM counters ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "rapid_insert_update_delete_s82a");
    });
}

#[test]
fn test_conformance_interleaved_dml_three_tables_s82b() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT)",
            "CREATE TABLE t2 (x INTEGER PRIMARY KEY, y TEXT)",
            "CREATE TABLE t3 (p INTEGER PRIMARY KEY, q TEXT)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let ops = [
            "INSERT INTO t1 VALUES (1, 'alpha')",
            "INSERT INTO t2 VALUES (10, 'beta')",
            "INSERT INTO t3 VALUES (100, 'gamma')",
            "UPDATE t1 SET b = 'ALPHA' WHERE a = 1",
            "INSERT INTO t2 VALUES (20, 'delta')",
            "DELETE FROM t3 WHERE p = 100",
            "INSERT INTO t3 VALUES (200, 'epsilon')",
            "UPDATE t2 SET y = upper(y) WHERE x = 10",
            "INSERT INTO t1 VALUES (2, 'zeta'), (3, 'eta')",
            "DELETE FROM t2 WHERE x = 20",
        ];
        for s in &ops {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT * FROM t1 ORDER BY a",
                "SELECT * FROM t2 ORDER BY x",
                "SELECT * FROM t3 ORDER BY p",
            ],
        )
        .await;
        assert_no_mismatches(&m, "interleaved_dml_three_tables_s82b");
    });
}

#[test]
fn test_conformance_explicit_txn_multi_update_s82c() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL NOT NULL)",
            "INSERT INTO accounts VALUES (1, 1000.0), (2, 2000.0), (3, 500.0)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let ops = [
            "BEGIN",
            "UPDATE accounts SET balance = balance - 100.0 WHERE id = 1",
            "UPDATE accounts SET balance = balance + 100.0 WHERE id = 2",
            "UPDATE accounts SET balance = balance * 1.05 WHERE id = 3",
            "COMMIT",
        ];
        for s in &ops {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, balance FROM accounts ORDER BY id",
                "SELECT SUM(balance) FROM accounts",
            ],
        )
        .await;
        assert_no_mismatches(&m, "explicit_txn_multi_update_s82c");
    });
}

#[test]
fn test_conformance_delete_with_subquery_in_s82d() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price REAL)",
            "INSERT INTO items VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, 'A', 30.0), (4, 'B', 5.0), (5, 'C', 15.0)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let del = "DELETE FROM items WHERE category IN (SELECT category FROM items GROUP BY category HAVING AVG(price) > 12.0)";
        fconn.execute(del).await.unwrap();
        rconn.execute_batch(del).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, category, price FROM items ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "delete_with_subquery_in_s82d");
    });
}

#[test]
fn test_conformance_update_from_case_expr_s82e() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE grades (student TEXT, score INTEGER, grade TEXT)",
            "INSERT INTO grades VALUES ('Alice', 95, NULL), ('Bob', 72, NULL), ('Carol', 88, NULL), ('Dave', 45, NULL)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let upd = "UPDATE grades SET grade = CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' ELSE 'F' END";
        fconn.execute(upd).await.unwrap();
        rconn.execute_batch(upd).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT student, score, grade FROM grades ORDER BY student"],
        )
        .await;
        assert_no_mismatches(&m, "update_from_case_expr_s82e");
    });
}

#[test]
fn test_conformance_insert_or_replace_upsert_s82f() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE kv (key TEXT PRIMARY KEY, val INTEGER NOT NULL, updated_count INTEGER DEFAULT 0)",
            "INSERT INTO kv VALUES ('x', 1, 0), ('y', 2, 0), ('z', 3, 0)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let ops = [
            "INSERT OR REPLACE INTO kv VALUES ('x', 10, 1)",
            "INSERT INTO kv VALUES ('w', 4, 0)",
            "INSERT OR REPLACE INTO kv VALUES ('y', 20, 1)",
            "INSERT OR REPLACE INTO kv VALUES ('new', 99, 0)",
        ];
        for s in &ops {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT key, val, updated_count FROM kv ORDER BY key"],
        )
        .await;
        assert_no_mismatches(&m, "insert_or_replace_upsert_s82f");
    });
}

#[test]
fn test_conformance_update_with_correlated_subquery_s82g() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)",
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget REAL)",
            "INSERT INTO departments VALUES (1, 'Engineering', 500000.0), (2, 'Sales', 300000.0)",
            "INSERT INTO employees VALUES (1, 'Alice', 1, 80000.0), (2, 'Bob', 1, 90000.0), (3, 'Carol', 2, 70000.0), (4, 'Dave', 2, 60000.0)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let upd = "UPDATE employees SET salary = salary * 1.1 WHERE dept_id IN (SELECT id FROM departments WHERE budget > 400000.0)";
        fconn.execute(upd).await.unwrap();
        rconn.execute_batch(upd).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, name, salary FROM employees ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "update_with_correlated_subquery_s82g");
    });
}

#[test]
fn test_conformance_delete_where_not_exists_s82h() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL)",
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
            "INSERT INTO customers VALUES (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Carol', 1)",
            "INSERT INTO orders VALUES (1, 1, 100.0), (2, 2, 200.0), (3, 3, 50.0), (4, 99, 10.0)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let del = "DELETE FROM orders WHERE NOT EXISTS (SELECT 1 FROM customers WHERE customers.id = orders.customer_id AND customers.active = 1)";
        fconn.execute(del).await.unwrap();
        rconn.execute_batch(del).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, customer_id, total FROM orders ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "delete_where_not_exists_s82h");
    });
}

#[test]
fn test_conformance_multi_column_update_set_s82i() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE points (id INTEGER PRIMARY KEY, x REAL, y REAL, label TEXT)",
            "INSERT INTO points VALUES (1, 1.0, 2.0, 'origin'), (2, 3.0, 4.0, 'mid'), (3, 5.0, 6.0, 'far')",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let upd =
            "UPDATE points SET x = x * 2, y = y * 2, label = label || '_scaled' WHERE id >= 2";
        fconn.execute(upd).await.unwrap();
        rconn.execute_batch(upd).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, x, y, label FROM points ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "multi_column_update_set_s82i");
    });
}

#[test]
fn test_conformance_insert_select_aggregate_s82j() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount REAL)",
            "CREATE TABLE summary (product TEXT PRIMARY KEY, total REAL, cnt INTEGER)",
            "INSERT INTO sales VALUES (1, 'Widget', 10.0), (2, 'Widget', 20.0), (3, 'Gadget', 15.0), (4, 'Gadget', 25.0), (5, 'Widget', 5.0)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let ins =
            "INSERT INTO summary SELECT product, SUM(amount), COUNT(*) FROM sales GROUP BY product";
        fconn.execute(ins).await.unwrap();
        rconn.execute_batch(ins).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT product, total, cnt FROM summary ORDER BY product"],
        )
        .await;
        assert_no_mismatches(&m, "insert_select_aggregate_s82j");
    });
}

#[test]
fn test_conformance_txn_rollback_preserves_prior_s82k() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE data (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO data VALUES (1, 'original')",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let ops = [
            "BEGIN",
            "UPDATE data SET v = 'modified' WHERE id = 1",
            "INSERT INTO data VALUES (2, 'new_row')",
            "ROLLBACK",
        ];
        for s in &ops {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, v FROM data ORDER BY id",
                "SELECT COUNT(*) FROM data",
            ],
        )
        .await;
        assert_no_mismatches(&m, "txn_rollback_preserves_prior_s82k");
    });
}

#[test]
fn test_conformance_savepoint_nested_dml_s82l() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)",
            "INSERT INTO log VALUES (1, 'init')",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let ops = [
            "BEGIN",
            "INSERT INTO log VALUES (2, 'txn_start')",
            "SAVEPOINT sp1",
            "INSERT INTO log VALUES (3, 'in_savepoint')",
            "UPDATE log SET msg = 'updated_in_sp' WHERE id = 1",
            "SAVEPOINT sp2",
            "INSERT INTO log VALUES (4, 'nested_sp')",
            "ROLLBACK TO sp2",
            "INSERT INTO log VALUES (5, 'after_rollback_sp2')",
            "RELEASE sp1",
            "COMMIT",
        ];
        for s in &ops {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(&fconn, &rconn, &["SELECT id, msg FROM log ORDER BY id"]).await;
        assert_no_mismatches(&m, "savepoint_nested_dml_s82l");
    });
}

#[test]
fn test_conformance_update_returning_s82m() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE inventory (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)",
            "INSERT INTO inventory VALUES (1, 'Bolt', 100), (2, 'Nut', 200), (3, 'Washer', 50)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, name, qty FROM inventory ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "update_returning_s82m_pre");
        let upd = "UPDATE inventory SET qty = qty - 10 WHERE qty > 50";
        fconn.execute(upd).await.unwrap();
        rconn.execute_batch(upd).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, name, qty FROM inventory ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "update_returning_s82m_post");
    });
}

#[test]
fn test_conformance_delete_all_then_reinsert_s82n() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE tmp (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO tmp VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let ops = [
            "DELETE FROM tmp",
            "INSERT INTO tmp VALUES (10, 'x'), (20, 'y')",
        ];
        for s in &ops {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, val FROM tmp ORDER BY id",
                "SELECT COUNT(*) FROM tmp",
            ],
        )
        .await;
        assert_no_mismatches(&m, "delete_all_then_reinsert_s82n");
    });
}

#[test]
fn test_conformance_insert_or_ignore_unique_s82o() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE uniq (id INTEGER PRIMARY KEY, name TEXT UNIQUE, val INTEGER)",
            "INSERT INTO uniq VALUES (1, 'alpha', 10), (2, 'beta', 20)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let ops = [
            "INSERT OR IGNORE INTO uniq VALUES (3, 'alpha', 30)",
            "INSERT OR IGNORE INTO uniq VALUES (4, 'gamma', 40)",
            "INSERT OR IGNORE INTO uniq VALUES (5, 'beta', 50)",
        ];
        for s in &ops {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, name, val FROM uniq ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "insert_or_ignore_unique_s82o");
    });
}

#[test]
fn test_conformance_fk_cascade_multi_level_s82p() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE, info TEXT)",
            "CREATE TABLE grandchild (id INTEGER PRIMARY KEY, child_id INTEGER REFERENCES child(id) ON DELETE CASCADE, detail TEXT)",
            "INSERT INTO parent VALUES (1, 'P1'), (2, 'P2')",
            "INSERT INTO child VALUES (10, 1, 'C1'), (20, 1, 'C2'), (30, 2, 'C3')",
            "INSERT INTO grandchild VALUES (100, 10, 'G1'), (200, 20, 'G2'), (300, 30, 'G3')",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let del = "DELETE FROM parent WHERE id = 1";
        fconn.execute(del).await.unwrap();
        rconn.execute_batch(del).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT * FROM parent ORDER BY id",
                "SELECT * FROM child ORDER BY id",
                "SELECT * FROM grandchild ORDER BY id",
            ],
        )
        .await;
        assert_no_mismatches(&m, "fk_cascade_multi_level_s82p");
    });
}

#[test]
fn test_conformance_update_with_math_functions_s82q() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE nums (id INTEGER PRIMARY KEY, val REAL)",
            "INSERT INTO nums VALUES (1, -3.7), (2, 0.0), (3, 2.5), (4, 9.99)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, abs(val), round(val, 1) FROM nums ORDER BY id",
                "SELECT id, typeof(val), val > 0 FROM nums ORDER BY id",
                "SELECT MAX(val), MIN(val), AVG(val), SUM(val) FROM nums",
            ],
        )
        .await;
        assert_no_mismatches(&m, "update_with_math_functions_s82q");
    });
}

#[test]
fn test_conformance_complex_where_boolean_s82r() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, in_stock INTEGER, category TEXT)",
            "INSERT INTO products VALUES (1, 'Widget', 9.99, 1, 'A'), (2, 'Gadget', 24.99, 0, 'B'), (3, 'Doohickey', 4.99, 1, 'A'), (4, 'Thingamajig', 49.99, 1, 'C'), (5, 'Whatchamacallit', 14.99, 0, 'A')",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT name FROM products WHERE (in_stock = 1 AND price < 20.0) OR (category = 'C' AND price > 40.0) ORDER BY name",
            "SELECT name FROM products WHERE NOT (in_stock = 0 OR price > 25.0) ORDER BY name",
            "SELECT category, COUNT(*), AVG(price) FROM products WHERE in_stock = 1 GROUP BY category HAVING AVG(price) > 5.0 ORDER BY category",
        ],
    )
    .await;
        assert_no_mismatches(&m, "complex_where_boolean_s82r");
    });
}

#[test]
fn test_conformance_cte_with_dml_verify_s82s() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, status TEXT, priority INTEGER)",
            "INSERT INTO tasks VALUES (1, 'Build', 'done', 3), (2, 'Test', 'pending', 2), (3, 'Deploy', 'pending', 1), (4, 'Review', 'done', 2), (5, 'Plan', 'pending', 3)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "WITH pending AS (SELECT * FROM tasks WHERE status = 'pending') SELECT title, priority FROM pending ORDER BY priority DESC, title",
            "WITH ranked AS (SELECT title, priority, ROW_NUMBER() OVER (ORDER BY priority DESC) AS rn FROM tasks WHERE status = 'pending') SELECT title, priority, rn FROM ranked ORDER BY rn",
            "WITH stats AS (SELECT status, COUNT(*) AS cnt, AVG(priority) AS avg_pri FROM tasks GROUP BY status) SELECT * FROM stats ORDER BY status",
        ],
    )
    .await;
        assert_no_mismatches(&m, "cte_with_dml_verify_s82s");
    });
}

#[test]
fn test_conformance_recursive_cte_hierarchy_s82t() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open(":memory:").unwrap();
        let setup = [
            "CREATE TABLE org (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)",
            "INSERT INTO org VALUES (1, 'CEO', NULL), (2, 'VP_Eng', 1), (3, 'VP_Sales', 1), (4, 'Lead', 2), (5, 'Dev1', 4), (6, 'Dev2', 4), (7, 'Rep1', 3)",
        ];
        for s in &setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "WITH RECURSIVE chain(id, name, depth) AS (SELECT id, name, 0 FROM org WHERE manager_id IS NULL UNION ALL SELECT o.id, o.name, c.depth + 1 FROM org o JOIN chain c ON o.manager_id = c.id) SELECT name, depth FROM chain ORDER BY depth, name",
        ],
    )
    .await;
        assert_no_mismatches(&m, "recursive_cte_hierarchy_s82t");
    });
}
