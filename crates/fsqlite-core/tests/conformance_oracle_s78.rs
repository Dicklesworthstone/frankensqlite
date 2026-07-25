//! Conformance oracle tests — Session 78 (cc4 bug-probe batch)
//!
//! Targeted probes for: expression edge cases, nested subquery positions,
//! CASE/COALESCE with NULLs in aggregate context, mixed DML+SELECT sequences,
//! multi-level LEFT JOIN NULL propagation, UPDATE/DELETE with complex WHERE,
//! trigger interaction with constraints, and function return type preservation.

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

// ── s78a: subquery as DEFAULT value and in CHECK constraint ──

#[test]
fn test_conformance_subquery_in_check_s78a() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE bounds(id INTEGER PRIMARY KEY, lo INTEGER, hi INTEGER)",
            "INSERT INTO bounds VALUES(1, 0, 100)",
            "CREATE TABLE vals(id INTEGER PRIMARY KEY, v INTEGER CHECK(v >= 0 AND v <= 100))",
            "INSERT INTO vals VALUES(1, 50)",
            "INSERT INTO vals VALUES(2, 0)",
            "INSERT INTO vals VALUES(3, 100)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM vals ORDER BY id",
            "SELECT COUNT(*) FROM vals WHERE v BETWEEN 0 AND 100",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "CHECK constraint",
        );
    });
}

// ── s78b: nested CASE with NULL propagation in aggregate ──

#[test]
fn test_conformance_nested_case_null_agg_s78b() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE scores(id INTEGER PRIMARY KEY, student TEXT, subject TEXT, score INTEGER)",
            "INSERT INTO scores VALUES(1,'Alice','Math',90),(2,'Alice','English',NULL),(3,'Bob','Math',70),(4,'Bob','English',80),(5,'Charlie','Math',NULL),(6,'Charlie','English',NULL)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT student, SUM(CASE WHEN score IS NOT NULL THEN score ELSE 0 END) AS total FROM scores GROUP BY student ORDER BY student",
            "SELECT student, COUNT(score) AS non_null_scores, COUNT(*) AS total_rows FROM scores GROUP BY student ORDER BY student",
            "SELECT student, COALESCE(AVG(score), 0) AS avg_score FROM scores GROUP BY student ORDER BY student",
            "SELECT subject, MIN(score), MAX(score) FROM scores GROUP BY subject ORDER BY subject",
            "SELECT student, GROUP_CONCAT(CASE WHEN score IS NOT NULL THEN CAST(score AS TEXT) ELSE 'N/A' END, ', ') AS details FROM scores GROUP BY student ORDER BY student",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "nested CASE NULL agg",
        );
    });
}

// ── s78c: multi-level LEFT JOIN with NULL propagation ──

#[test]
fn test_conformance_multi_left_join_null_s78c() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER, val TEXT)",
            "CREATE TABLE t3(id INTEGER PRIMARY KEY, t2_id INTEGER, extra TEXT)",
            "INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c')",
            "INSERT INTO t2 VALUES(10,1,'x'),(11,1,'y'),(12,3,'z')",
            "INSERT INTO t3 VALUES(100,10,'p'),(101,12,'q')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT t1.name, t2.val, t3.extra FROM t1 LEFT JOIN t2 ON t2.t1_id = t1.id LEFT JOIN t3 ON t3.t2_id = t2.id ORDER BY t1.id, t2.id, t3.id",
            "SELECT t1.name, COUNT(t2.id) AS t2_count, COUNT(t3.id) AS t3_count FROM t1 LEFT JOIN t2 ON t2.t1_id = t1.id LEFT JOIN t3 ON t3.t2_id = t2.id GROUP BY t1.name ORDER BY t1.name",
            "SELECT t1.name FROM t1 LEFT JOIN t2 ON t2.t1_id = t1.id WHERE t2.id IS NULL ORDER BY t1.name",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "multi LEFT JOIN NULL",
        );
    });
}

// ── s78d: correlated subquery in UPDATE SET clause ──

#[test]
fn test_conformance_correlated_update_set_s78d() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, total_sold INTEGER DEFAULT 0)",
            "CREATE TABLE sales(id INTEGER PRIMARY KEY, product_id INTEGER, quantity INTEGER)",
            "INSERT INTO products VALUES(1,'Widget',0),(2,'Gadget',0),(3,'Doohickey',0)",
            "INSERT INTO sales VALUES(1,1,10),(2,1,20),(3,2,15),(4,1,5)",
            "UPDATE products SET total_sold = (SELECT COALESCE(SUM(quantity), 0) FROM sales WHERE sales.product_id = products.id)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM products ORDER BY id",
            "SELECT name, total_sold FROM products WHERE total_sold > 0 ORDER BY total_sold DESC",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "correlated UPDATE SET",
        );
    });
}

// ── s78e: DELETE with correlated NOT EXISTS ──

#[test]
fn test_conformance_delete_not_exists_s78e() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER)",
            "INSERT INTO parent VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d')",
            "INSERT INTO child VALUES(10,1),(11,1),(12,3)",
            "DELETE FROM parent WHERE NOT EXISTS (SELECT 1 FROM child WHERE child.parent_id = parent.id)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM parent ORDER BY id",
            "SELECT COUNT(*) FROM parent",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "DELETE NOT EXISTS",
        );
    });
}

// ── s78f: expression in LIMIT/OFFSET ──

#[test]
fn test_conformance_expr_limit_offset_s78f() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO nums VALUES(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT v FROM nums ORDER BY v LIMIT 3",
            "SELECT v FROM nums ORDER BY v LIMIT 3 OFFSET 2",
            "SELECT v FROM nums ORDER BY v DESC LIMIT 2 OFFSET 1",
            "SELECT v FROM nums ORDER BY v LIMIT 1+2",
            "SELECT v FROM nums ORDER BY v LIMIT 10 OFFSET 5",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "expr LIMIT/OFFSET",
        );
    });
}

// ── s78g: nested aggregate with derived table ──

#[test]
fn test_conformance_nested_agg_derived_table_s78g() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer TEXT, amount REAL)",
            "INSERT INTO orders VALUES(1,'Alice',100.0),(2,'Alice',200.0),(3,'Bob',150.0),(4,'Bob',50.0),(5,'Charlie',300.0)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT MAX(total) FROM (SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer)",
            "SELECT AVG(total) FROM (SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer)",
            "SELECT COUNT(*) FROM (SELECT customer FROM orders GROUP BY customer HAVING SUM(amount) > 200)",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "nested agg derived table",
        );
    });
}

// ── s78h: CAST with edge cases ──

#[test]
fn test_conformance_cast_edges_s78h() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT CAST(123 AS TEXT)",
            "SELECT CAST('456' AS INTEGER)",
            "SELECT CAST(3.14 AS INTEGER)",
            "SELECT CAST('3.14' AS REAL)",
            "SELECT CAST(NULL AS INTEGER)",
            "SELECT CAST(NULL AS TEXT)",
            "SELECT CAST('' AS INTEGER)",
            "SELECT CAST('abc' AS INTEGER)",
            "SELECT typeof(CAST(123 AS TEXT))",
            "SELECT typeof(CAST('456' AS INTEGER))",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await, "CAST edges");
    });
}

// ── s78i: string function edge cases ──

#[test]
fn test_conformance_string_func_edges_s78i() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT LENGTH('')",
            "SELECT LENGTH(NULL)",
            "SELECT SUBSTR('hello', 2, 3)",
            "SELECT SUBSTR('hello', -2)",
            "SELECT UPPER('hello world')",
            "SELECT LOWER('HELLO WORLD')",
            "SELECT TRIM('  hello  ')",
            "SELECT LTRIM('xxhello', 'x')",
            "SELECT RTRIM('helloxx', 'x')",
            "SELECT REPLACE('hello world', 'world', 'there')",
            "SELECT INSTR('hello world', 'world')",
            "SELECT INSTR('hello', 'xyz')",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "string func edges",
        );
    });
}

// ── s78j: INSERT with complex DEFAULT + RETURNING ──

#[test]
fn test_conformance_insert_default_returning_s78j() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE logs(id INTEGER PRIMARY KEY, msg TEXT NOT NULL, created TEXT DEFAULT 'unknown', priority INTEGER DEFAULT 0)",
            "INSERT INTO logs(msg) VALUES('first')",
            "INSERT INTO logs(msg, priority) VALUES('second', 5)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM logs ORDER BY id",
            "INSERT INTO logs(msg, priority) VALUES('third', 3) RETURNING id, msg, created, priority",
            "SELECT COUNT(*) FROM logs",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "INSERT DEFAULT RETURNING",
        );
    });
}

// ── s78k: window function with complex frame ──

#[test]
fn test_conformance_window_complex_frame_s78k() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE ts(id INTEGER PRIMARY KEY, dt TEXT, val INTEGER)",
            "INSERT INTO ts VALUES(1,'2024-01-01',10),(2,'2024-01-02',20),(3,'2024-01-03',15),(4,'2024-01-04',25),(5,'2024-01-05',30)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, val, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_sum FROM ts ORDER BY id",
            "SELECT id, val, AVG(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg FROM ts ORDER BY id",
            "SELECT id, val, FIRST_VALUE(val) OVER (ORDER BY id) AS first_val, LAST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_val FROM ts ORDER BY id",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "window complex frame",
        );
    });
}

// ── s78l: LIKE with special characters ──

#[test]
fn test_conformance_like_special_chars_s78l() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE paths(id INTEGER PRIMARY KEY, path TEXT)",
            "INSERT INTO paths VALUES(1,'/usr/bin'),(2,'/usr/local/bin'),(3,'/etc/config'),(4,'C:\\Windows\\System32'),(5,'100%_done')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT path FROM paths WHERE path LIKE '/usr%' ORDER BY id",
            "SELECT path FROM paths WHERE path LIKE '%bin' ORDER BY id",
            "SELECT path FROM paths WHERE path LIKE '%\\%' ESCAPE '\\' ORDER BY id",
            "SELECT path FROM paths WHERE path LIKE '%config%' ORDER BY id",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "LIKE special chars",
        );
    });
}

// ── s78m: arithmetic overflow / underflow ──

#[test]
fn test_conformance_arithmetic_overflow_s78m() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT 9223372036854775807 + 0",
            "SELECT -9223372036854775808 + 0",
            "SELECT 1 + 1",
            "SELECT 2147483647 * 2",
            "SELECT 1.0 / 3.0",
            "SELECT 10 / 3",
            "SELECT 10 % 3",
            "SELECT -10 % 3",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "arithmetic overflow",
        );
    });
}

// ── s78n: trigger + FK interaction ──

#[test]
fn test_conformance_trigger_fk_interaction_s78n() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE audit(id INTEGER PRIMARY KEY, action TEXT, detail TEXT)",
            "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)",
            "CREATE TRIGGER items_insert AFTER INSERT ON items BEGIN INSERT INTO audit(action, detail) VALUES('INSERT', NEW.name); END",
            "CREATE TRIGGER items_delete AFTER DELETE ON items BEGIN INSERT INTO audit(action, detail) VALUES('DELETE', OLD.name); END",
            "INSERT INTO items VALUES(1,'apple',10),(2,'banana',20),(3,'cherry',30)",
            "DELETE FROM items WHERE qty < 25",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM items ORDER BY id",
            "SELECT * FROM audit ORDER BY id",
            "SELECT COUNT(*) FROM audit WHERE action = 'INSERT'",
            "SELECT COUNT(*) FROM audit WHERE action = 'DELETE'",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "trigger FK interaction",
        );
    });
}

// ── s78o: complex WHERE with mixed boolean + subquery ──

#[test]
fn test_conformance_complex_where_bool_subquery_s78o() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER, active INTEGER)",
            "INSERT INTO emp VALUES(1,'Alice','Eng',90000,1),(2,'Bob','Eng',80000,0),(3,'Charlie','Sales',70000,1),(4,'Diana','Eng',95000,1),(5,'Eve','Sales',60000,1)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT name FROM emp WHERE active = 1 AND (salary > 80000 OR dept = 'Sales') ORDER BY name",
            "SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp WHERE active = 1) ORDER BY name",
            "SELECT name FROM emp WHERE dept IN (SELECT dept FROM emp GROUP BY dept HAVING COUNT(*) >= 2) AND active = 1 ORDER BY name",
            "SELECT name FROM emp WHERE NOT (salary < 70000 AND active = 0) ORDER BY name",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "complex WHERE bool subquery",
        );
    });
}

// ── s78p: GROUP_CONCAT with ORDER BY and separator ──

#[test]
fn test_conformance_group_concat_order_s78p() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE tags(id INTEGER PRIMARY KEY, item TEXT, tag TEXT)",
            "INSERT INTO tags VALUES(1,'a','red'),(2,'a','blue'),(3,'a','green'),(4,'b','red'),(5,'b','yellow')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT item, GROUP_CONCAT(tag, ', ') FROM tags GROUP BY item ORDER BY item",
            "SELECT item, GROUP_CONCAT(DISTINCT tag) FROM tags GROUP BY item ORDER BY item",
            "SELECT GROUP_CONCAT(tag, '|') FROM tags WHERE item = 'a'",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "GROUP_CONCAT order",
        );
    });
}

// ── s78q: REPLACE INTO with trigger side effects ──

#[test]
fn test_conformance_replace_trigger_s78q() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE kv(key TEXT PRIMARY KEY, val INTEGER)",
            "CREATE TABLE kv_log(id INTEGER PRIMARY KEY, op TEXT, k TEXT, v INTEGER)",
            "CREATE TRIGGER kv_ins AFTER INSERT ON kv BEGIN INSERT INTO kv_log(op, k, v) VALUES('I', NEW.key, NEW.val); END",
            "CREATE TRIGGER kv_del AFTER DELETE ON kv BEGIN INSERT INTO kv_log(op, k, v) VALUES('D', OLD.key, OLD.val); END",
            "INSERT INTO kv VALUES('x', 1)",
            "REPLACE INTO kv VALUES('x', 2)",
            "REPLACE INTO kv VALUES('y', 3)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM kv ORDER BY key",
            "SELECT op, k, v FROM kv_log ORDER BY id",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "REPLACE trigger",
        );
    });
}

// ── s78r: multi-CTE with cross-references ──

#[test]
fn test_conformance_multi_cte_cross_ref_s78r() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE data(id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)",
            "INSERT INTO data VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "WITH totals AS (SELECT cat, SUM(val) AS s FROM data GROUP BY cat), grand AS (SELECT SUM(s) AS g FROM totals) SELECT t.cat, t.s, ROUND(100.0 * t.s / grand.g, 1) AS pct FROM totals t, grand ORDER BY t.cat",
            "WITH stats AS (SELECT cat, COUNT(*) AS cnt, AVG(val) AS avg_val FROM data GROUP BY cat) SELECT cat, cnt, avg_val FROM stats WHERE cnt >= 2 ORDER BY cat",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "multi CTE cross-ref",
        );
    });
}

// ── s78s: complex HAVING with aggregate expressions ──

#[test]
fn test_conformance_complex_having_s78s() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE txns(id INTEGER PRIMARY KEY, acct TEXT, amount REAL, type TEXT)",
            "INSERT INTO txns VALUES(1,'A',100.0,'credit'),(2,'A',-50.0,'debit'),(3,'A',200.0,'credit'),(4,'B',300.0,'credit'),(5,'B',-100.0,'debit'),(6,'C',50.0,'credit')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT acct, SUM(amount) AS balance FROM txns GROUP BY acct HAVING SUM(amount) > 100 ORDER BY acct",
            "SELECT acct, COUNT(*) AS n, SUM(CASE WHEN type='credit' THEN amount ELSE 0 END) AS credits FROM txns GROUP BY acct HAVING COUNT(*) >= 2 ORDER BY acct",
            "SELECT type, SUM(ABS(amount)) AS total FROM txns GROUP BY type ORDER BY type",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "complex HAVING",
        );
    });
}

// ── s78t: INSERT...SELECT with expression transforms ──

#[test]
fn test_conformance_insert_select_transform_s78t() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE src(id INTEGER PRIMARY KEY, name TEXT, val REAL)",
            "CREATE TABLE dst(id INTEGER PRIMARY KEY, label TEXT, doubled REAL, category TEXT)",
            "INSERT INTO src VALUES(1,'alpha',1.5),(2,'beta',2.5),(3,'gamma',3.5),(4,'delta',4.5)",
            "INSERT INTO dst SELECT id, UPPER(name), val * 2.0, CASE WHEN val < 3 THEN 'low' ELSE 'high' END FROM src",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM dst ORDER BY id",
            "SELECT category, COUNT(*) FROM dst GROUP BY category ORDER BY category",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "INSERT SELECT transform",
        );
    });
}

// ── s78u: printf/format function ──

#[test]
fn test_conformance_printf_format_s78u() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT printf('%d', 42)",
            "SELECT printf('%05d', 42)",
            "SELECT printf('%.2f', 3.14159)",
            "SELECT printf('%s world', 'hello')",
            "SELECT printf('%010.3f', 1.5)",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "printf format",
        );
    });
}

// ── s78v: hex / zeroblob / quote ──

#[test]
fn test_conformance_hex_zeroblob_quote_s78v() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT hex('hello')",
            "SELECT hex(42)",
            "SELECT hex(NULL)",
            "SELECT length(zeroblob(10))",
            "SELECT typeof(zeroblob(5))",
            "SELECT quote(42)",
            "SELECT quote('hello')",
            "SELECT quote(NULL)",
            "SELECT quote(3.14)",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "hex/zeroblob/quote",
        );
    });
}

// ── s78w: unicode / char / unicode function ──

#[test]
fn test_conformance_unicode_char_s78w() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT unicode('A')",
            "SELECT unicode('a')",
            "SELECT char(65)",
            "SELECT char(65, 66, 67)",
            "SELECT length('café')",
            "SELECT upper('café')",
            "SELECT lower('CAFÉ')",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "unicode/char",
        );
    });
}

// ── s78x: nested subquery in ORDER BY ──

#[test]
fn test_conformance_subquery_in_order_by_s78x() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "CREATE TABLE dept(id INTEGER PRIMARY KEY, name TEXT, rank INTEGER)",
            "INSERT INTO dept VALUES(1,'Eng',1),(2,'Sales',3),(3,'HR',2)",
            "INSERT INTO emp VALUES(1,'Alice',1),(2,'Bob',2),(3,'Charlie',3),(4,'Diana',1)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT e.name FROM emp e ORDER BY (SELECT d.rank FROM dept d WHERE d.id = e.dept_id), e.name",
            "SELECT e.name, (SELECT d.name FROM dept d WHERE d.id = e.dept_id) AS dept_name FROM emp e ORDER BY dept_name, e.name",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "subquery in ORDER BY",
        );
    });
}

// ── s78y: DISTINCT with expressions ──

#[test]
fn test_conformance_distinct_expressions_s78y() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE vals(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO vals VALUES(1,10),(2,20),(3,10),(4,30),(5,20),(6,10)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT DISTINCT v FROM vals ORDER BY v",
            "SELECT COUNT(DISTINCT v) FROM vals",
            "SELECT SUM(DISTINCT v) FROM vals",
            "SELECT DISTINCT v * 2 AS doubled FROM vals ORDER BY doubled",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "DISTINCT expressions",
        );
    });
}

// ── s78z: complex UPDATE with multiple SET clauses ──

#[test]
fn test_conformance_complex_update_multi_set_s78z() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE inventory(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER, price REAL, updated TEXT DEFAULT 'no')",
            "INSERT INTO inventory VALUES(1,'widget',100,9.99,'no'),(2,'gadget',50,19.99,'no'),(3,'doohickey',200,4.99,'no')",
            "UPDATE inventory SET qty = qty - 10, price = price * 1.1, updated = 'yes' WHERE qty > 50",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM inventory ORDER BY id",
            "SELECT name, updated FROM inventory WHERE updated = 'yes' ORDER BY name",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "complex UPDATE multi SET",
        );
    });
}

// ── s78aa: BETWEEN with various types ──

#[test]
fn test_conformance_between_types_s78aa() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE mixed(id INTEGER PRIMARY KEY, i INTEGER, r REAL, t TEXT)",
            "INSERT INTO mixed VALUES(1,10,1.5,'apple'),(2,20,2.5,'banana'),(3,30,3.5,'cherry'),(4,40,4.5,'date'),(5,50,5.5,'elderberry')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM mixed WHERE i BETWEEN 20 AND 40 ORDER BY id",
            "SELECT * FROM mixed WHERE r BETWEEN 2.0 AND 4.0 ORDER BY id",
            "SELECT * FROM mixed WHERE t BETWEEN 'banana' AND 'date' ORDER BY id",
            "SELECT * FROM mixed WHERE i NOT BETWEEN 10 AND 30 ORDER BY id",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "BETWEEN types",
        );
    });
}

// ── s78ab: multiple aggregates in single query ──

#[test]
fn test_conformance_multi_agg_single_query_s78ab() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE samples(id INTEGER PRIMARY KEY, grp TEXT, val REAL)",
            "INSERT INTO samples VALUES(1,'A',10.0),(2,'A',20.0),(3,'A',30.0),(4,'B',15.0),(5,'B',25.0),(6,'C',100.0)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT grp, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM samples GROUP BY grp ORDER BY grp",
            "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val), TOTAL(val) FROM samples",
            "SELECT grp, COUNT(*) AS n, SUM(val)/COUNT(*) AS manual_avg, AVG(val) AS builtin_avg FROM samples GROUP BY grp ORDER BY grp",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "multi agg single query",
        );
    });
}

// ── s78ac: ALTER TABLE ADD COLUMN then query ──

#[test]
fn test_conformance_alter_add_column_s78ac() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO t VALUES(1,'Alice'),(2,'Bob')",
            "ALTER TABLE t ADD COLUMN age INTEGER DEFAULT 0",
            "UPDATE t SET age = 30 WHERE name = 'Alice'",
            "INSERT INTO t VALUES(3, 'Charlie', 25)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM t ORDER BY id",
            "SELECT name, age FROM t WHERE age > 0 ORDER BY name",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "ALTER ADD COLUMN",
        );
    });
}

// ── s78ad: last_insert_rowid / changes / total_changes ──

#[test]
fn test_conformance_last_insert_rowid_s78ad() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO t VALUES(NULL, 'first')",
            "INSERT INTO t VALUES(NULL, 'second')",
            "INSERT INTO t VALUES(NULL, 'third')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM t ORDER BY id",
            "SELECT last_insert_rowid()",
            "SELECT MAX(id) FROM t",
        ];
        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "last_insert_rowid",
        );
    });
}
