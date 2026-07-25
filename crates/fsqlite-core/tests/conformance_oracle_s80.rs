//! Conformance oracle tests — Session 80 (cc4 UPDATE/DELETE ceremony + edge cases)
//!
//! Targeted probes: UPDATE with complex SET expressions, DELETE with
//! multi-predicate WHERE, prepared-statement reuse patterns, PRAGMA
//! interactions, and edge-case DML scenarios.

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

// ── s80a: UPDATE with arithmetic SET expressions ──

#[test]
fn test_conformance_update_arithmetic_set_s80a() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE inventory(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER, price REAL)",
            "INSERT INTO inventory VALUES(1,'Widget',100,9.99),(2,'Gadget',50,19.99),(3,'Doohickey',200,4.99)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        for dml in &[
            "UPDATE inventory SET qty = qty * 2, price = price * 0.9 WHERE qty < 100",
            "UPDATE inventory SET qty = qty + 10 WHERE price > 10.0",
        ] {
            fconn.execute(dml).await.unwrap();
            rconn.execute_batch(dml).unwrap();
        }

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, name, qty, price FROM inventory ORDER BY id"],
            )
            .await,
            "UPDATE arithmetic SET",
        );
    });
}

// ── s80b: DELETE with complex multi-predicate WHERE ──

#[test]
fn test_conformance_delete_complex_where_s80b() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE logs(id INTEGER PRIMARY KEY, level TEXT, msg TEXT, ts INTEGER)",
            "INSERT INTO logs VALUES(1,'INFO','start',1000),(2,'WARN','slow',1001),(3,'ERROR','fail',1002)",
            "INSERT INTO logs VALUES(4,'INFO','ok',1003),(5,'DEBUG','trace',1004),(6,'ERROR','crash',1005)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        {
            let dml = "DELETE FROM logs WHERE level = 'DEBUG' OR (level = 'INFO' AND ts < 1002)";
            fconn.execute(dml).await.unwrap();
            rconn.execute_batch(dml).unwrap();
        }

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, level, msg FROM logs ORDER BY id"],
            )
            .await,
            "DELETE complex WHERE",
        );
    });
}

// ── s80c: UPDATE with CASE expression in SET ──

#[test]
fn test_conformance_update_case_set_s80c() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE grades(student TEXT, score INTEGER, grade TEXT)",
            "INSERT INTO grades VALUES('Alice',95,NULL),('Bob',72,NULL),('Charlie',88,NULL),('Diana',45,NULL)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml = "UPDATE grades SET grade = CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT student, score, grade FROM grades ORDER BY student"],
            )
            .await,
            "UPDATE CASE SET",
        );
    });
}

// ── s80d: Prepared statement reuse across multiple executions ──

#[test]
fn test_conformance_prepared_reuse_s80d() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &["CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)"];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        fconn.execute("BEGIN").await.unwrap();
        rconn.execute_batch("BEGIN").unwrap();

        let f_stmt = fconn
            .prepare("INSERT INTO kv VALUES(?1, ?2)")
            .await
            .unwrap();
        let mut r_stmt = rconn.prepare("INSERT INTO kv VALUES(?1, ?2)").unwrap();

        for i in 0..20 {
            let v = format!("val_{i}");
            f_stmt
                .execute_with_params(&[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(v.clone().into()),
                ])
                .await
                .unwrap();
            r_stmt.execute(rusqlite::params![i, v]).unwrap();
        }

        fconn.execute("COMMIT").await.unwrap();
        rconn.execute_batch("COMMIT").unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &[
                    "SELECT COUNT(*) FROM kv",
                    "SELECT k, v FROM kv WHERE k IN (0, 5, 10, 15, 19) ORDER BY k",
                ],
            )
            .await,
            "prepared reuse INSERT",
        );
    });
}

// ── s80e: UPDATE with subquery in WHERE ──

#[test]
fn test_conformance_update_subquery_where_s80e() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, status TEXT)",
            "CREATE TABLE vips(customer_id INTEGER PRIMARY KEY)",
            "INSERT INTO orders VALUES(1,10,100.0,'pending'),(2,20,200.0,'pending'),(3,10,50.0,'pending'),(4,30,300.0,'pending')",
            "INSERT INTO vips VALUES(10),(30)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml = "UPDATE orders SET status = 'priority' WHERE customer_id IN (SELECT customer_id FROM vips)";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, customer_id, status FROM orders ORDER BY id"],
            )
            .await,
            "UPDATE subquery WHERE",
        );
    });
}

// ── s80f: DELETE with EXISTS subquery ──

#[test]
fn test_conformance_delete_exists_s80f() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE discontinued(product_id INTEGER)",
            "INSERT INTO products VALUES(1,'Alpha'),(2,'Beta'),(3,'Gamma'),(4,'Delta')",
            "INSERT INTO discontinued VALUES(2),(4)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml = "DELETE FROM products WHERE EXISTS (SELECT 1 FROM discontinued WHERE discontinued.product_id = products.id)";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, name FROM products ORDER BY id"],
            )
            .await,
            "DELETE EXISTS subquery",
        );
    });
}

// ── s80g: Multiple UPDATEs in single transaction ──

#[test]
fn test_conformance_multi_update_txn_s80g() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE accounts(id INTEGER PRIMARY KEY, name TEXT, balance REAL)",
            "INSERT INTO accounts VALUES(1,'Alice',1000.0),(2,'Bob',500.0),(3,'Charlie',750.0)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dmls = &[
            "BEGIN",
            "UPDATE accounts SET balance = balance - 200.0 WHERE id = 1",
            "UPDATE accounts SET balance = balance + 200.0 WHERE id = 2",
            "UPDATE accounts SET balance = balance * 1.1 WHERE id = 3",
            "COMMIT",
        ];
        for dml in dmls {
            fconn.execute(dml).await.unwrap();
            rconn.execute_batch(dml).unwrap();
        }

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &[
                    "SELECT id, name, balance FROM accounts ORDER BY id",
                    "SELECT SUM(balance) FROM accounts",
                ],
            )
            .await,
            "multi UPDATE txn",
        );
    });
}

// ── s80h: INSERT OR REPLACE with complex values ──

#[test]
fn test_conformance_insert_or_replace_complex_s80h() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE config(key TEXT PRIMARY KEY, value TEXT, updated_at INTEGER)",
            "INSERT INTO config VALUES('theme','dark',1000),('lang','en',1001),('tz','UTC',1002)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dmls = &[
            "INSERT OR REPLACE INTO config VALUES('theme','light',2000)",
            "INSERT OR REPLACE INTO config VALUES('font','mono',2001)",
            "INSERT OR REPLACE INTO config VALUES('lang','fr',2002)",
        ];
        for dml in dmls {
            fconn.execute(dml).await.unwrap();
            rconn.execute_batch(dml).unwrap();
        }

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT key, value, updated_at FROM config ORDER BY key"],
            )
            .await,
            "INSERT OR REPLACE complex",
        );
    });
}

// ── s80i: UPDATE RETURNING ──

#[test]
fn test_conformance_update_returning_s80i() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE tasks(id INTEGER PRIMARY KEY, title TEXT, done INTEGER DEFAULT 0)",
            "INSERT INTO tasks(title) VALUES('task1'),('task2'),('task3'),('task4')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["UPDATE tasks SET done = 1 WHERE id <= 2 RETURNING id, title, done"],
            )
            .await,
            "UPDATE RETURNING",
        );
    });
}

// ── s80j: DELETE RETURNING with expression ──

#[test]
fn test_conformance_delete_returning_expr_s80j() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, price REAL)",
            "INSERT INTO items VALUES(1,'A',10.0),(2,'B',20.0),(3,'C',30.0),(4,'D',5.0)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        assert_no_mismatches(
        &oracle_compare(
            &fconn,
            &rconn,
            &["DELETE FROM items WHERE price < 15.0 RETURNING id, name, price * 1.1 AS discounted"],
        )
        .await,
        "DELETE RETURNING expr",
    );
    });
}

// ── s80k: UPDATE multiple columns with mixed types ──

#[test]
fn test_conformance_update_mixed_types_s80k() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE mixed(id INTEGER PRIMARY KEY, txt TEXT, num REAL, flag INTEGER)",
            "INSERT INTO mixed VALUES(1,'hello',3.14,0),(2,'world',2.72,1),(3,'test',1.41,0)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml = "UPDATE mixed SET txt = upper(txt), num = round(num, 1), flag = 1 - flag";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, txt, num, flag FROM mixed ORDER BY id"],
            )
            .await,
            "UPDATE mixed types",
        );
    });
}

// ── s80l: DELETE all rows then verify empty table ──

#[test]
fn test_conformance_delete_all_s80l() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE temp_data(id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO temp_data VALUES(1,'a'),(2,'b'),(3,'c')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        fconn.execute("DELETE FROM temp_data").await.unwrap();
        rconn.execute_batch("DELETE FROM temp_data").unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &[
                    "SELECT COUNT(*) FROM temp_data",
                    "SELECT * FROM temp_data",
                    "INSERT INTO temp_data VALUES(10,'new')",
                ],
            )
            .await,
            "DELETE all rows",
        );

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, &["SELECT id, val FROM temp_data"]).await,
            "after re-insert",
        );
    });
}

// ── s80m: UPDATE with string function in SET ──

#[test]
fn test_conformance_update_string_fn_s80m() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE names(id INTEGER PRIMARY KEY, first TEXT, last TEXT, full_name TEXT)",
            "INSERT INTO names VALUES(1,'john','doe',NULL),(2,'jane','SMITH',NULL),(3,'BOB','Jones',NULL)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml = "UPDATE names SET full_name = upper(substr(first,1,1)) || lower(substr(first,2)) || ' ' || upper(substr(last,1,1)) || lower(substr(last,2))";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, full_name FROM names ORDER BY id"],
            )
            .await,
            "UPDATE string fn SET",
        );
    });
}

// ── s80n: INSERT ... SELECT with GROUP BY ──

#[test]
fn test_conformance_insert_select_group_by_s80n() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE sales(region TEXT, amount REAL)",
            "INSERT INTO sales VALUES('East',100),('West',200),('East',150),('West',50),('East',75)",
            "CREATE TABLE summary(region TEXT PRIMARY KEY, total REAL, cnt INTEGER)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml =
            "INSERT INTO summary SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT region, total, cnt FROM summary ORDER BY region"],
            )
            .await,
            "INSERT SELECT GROUP BY",
        );
    });
}

// ── s80o: UPDATE with rowid reference ──

#[test]
fn test_conformance_update_rowid_ref_s80o() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE t(val TEXT)",
            "INSERT INTO t VALUES('a'),('b'),('c'),('d'),('e')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml = "UPDATE t SET val = val || '_' || rowid WHERE rowid % 2 = 0";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, &["SELECT rowid, val FROM t ORDER BY rowid"]).await,
            "UPDATE rowid ref",
        );
    });
}

// ── s80p: DELETE with BETWEEN ──

#[test]
fn test_conformance_delete_between_s80p() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE numbers(id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO numbers VALUES(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        fconn
            .execute("DELETE FROM numbers WHERE val BETWEEN 30 AND 70")
            .await
            .unwrap();
        rconn
            .execute_batch("DELETE FROM numbers WHERE val BETWEEN 30 AND 70")
            .unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &[
                    "SELECT id, val FROM numbers ORDER BY id",
                    "SELECT COUNT(*) FROM numbers",
                ],
            )
            .await,
            "DELETE BETWEEN",
        );
    });
}

// ── s80q: UPDATE with NULL handling ──

#[test]
fn test_conformance_update_null_handling_s80q() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE nullable(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)",
            "INSERT INTO nullable VALUES(1,'x',10,1.0),(2,NULL,NULL,NULL),(3,'y',20,2.0),(4,NULL,30,NULL)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dmls = &[
            "UPDATE nullable SET a = COALESCE(a, 'default') WHERE a IS NULL",
            "UPDATE nullable SET c = COALESCE(c, 0.0)",
        ];
        for dml in dmls {
            fconn.execute(dml).await.unwrap();
            rconn.execute_batch(dml).unwrap();
        }

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, a, b, c FROM nullable ORDER BY id"],
            )
            .await,
            "UPDATE NULL handling",
        );
    });
}

// ── s80r: Interleaved INSERT/UPDATE/DELETE sequence ──

#[test]
fn test_conformance_interleaved_dml_s80r() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &["CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)"];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dmls = &[
            "INSERT INTO t VALUES(1,100),(2,200),(3,300)",
            "UPDATE t SET v = v + 10 WHERE id = 2",
            "DELETE FROM t WHERE id = 1",
            "INSERT INTO t VALUES(4,400),(5,500)",
            "UPDATE t SET v = v * 2 WHERE id >= 4",
            "DELETE FROM t WHERE v > 500",
            "INSERT INTO t VALUES(6,600)",
        ];
        for dml in dmls {
            fconn.execute(dml).await.unwrap();
            rconn.execute_batch(dml).unwrap();
        }

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &[
                    "SELECT id, v FROM t ORDER BY id",
                    "SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM t",
                ],
            )
            .await,
            "interleaved DML",
        );
    });
}

// ── s80s: UPDATE with IN list ──

#[test]
fn test_conformance_update_in_list_s80s() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, role TEXT DEFAULT 'user')",
            "INSERT INTO users(id, name) VALUES(1,'Alice'),(2,'Bob'),(3,'Charlie'),(4,'Diana'),(5,'Eve')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml = "UPDATE users SET role = 'admin' WHERE id IN (1, 3, 5)";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, name, role FROM users ORDER BY id"],
            )
            .await,
            "UPDATE IN list",
        );
    });
}

// ── s80t: DELETE with NOT IN subquery ──

#[test]
fn test_conformance_delete_not_in_subquery_s80t() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE all_items(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE keep_items(id INTEGER PRIMARY KEY)",
            "INSERT INTO all_items VALUES(1,'A'),(2,'B'),(3,'C'),(4,'D'),(5,'E')",
            "INSERT INTO keep_items VALUES(2),(4)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let dml = "DELETE FROM all_items WHERE id NOT IN (SELECT id FROM keep_items)";
        fconn.execute(dml).await.unwrap();
        rconn.execute_batch(dml).unwrap();

        assert_no_mismatches(
            &oracle_compare(
                &fconn,
                &rconn,
                &["SELECT id, name FROM all_items ORDER BY id"],
            )
            .await,
            "DELETE NOT IN subquery",
        );
    });
}
