//! Conformance oracle tests — Session 79 (cc4 COLLATE + aggregate probes)
//!
//! Targeted probes after fixing COLLATE NOCASE MIN/MAX (bd-ox2mg):
//! collation-aware aggregates, GROUP BY with COLLATE, ORDER BY COLLATE,
//! DISTINCT with COLLATE, nested aggregate expressions, COALESCE/IIF
//! with aggregates, and multi-table aggregate patterns.

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

// ── s79a: COLLATE NOCASE MIN/MAX with GROUP BY ──

#[test]
fn test_conformance_collate_nocase_min_max_group_by_s79a() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE products(category TEXT, name TEXT COLLATE NOCASE)",
            "INSERT INTO products VALUES('fruit','Apple'),('fruit','banana'),('fruit','CHERRY')",
            "INSERT INTO products VALUES('veg','Carrot'),('veg','broccoli'),('veg','DAIKON')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT category, MIN(name), MAX(name) FROM products GROUP BY category ORDER BY category",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "COLLATE NOCASE MIN/MAX GROUP BY",
        );
    });
}

// ── s79b: COLLATE NOCASE with COUNT(DISTINCT) ──

#[test]
fn test_conformance_collate_nocase_count_distinct_s79b() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE tags(tag TEXT COLLATE NOCASE)",
            "INSERT INTO tags VALUES('Rust'),('rust'),('RUST'),('Python'),('python')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &["SELECT COUNT(DISTINCT tag) FROM tags"];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "COLLATE NOCASE COUNT DISTINCT",
        );
    });
}

// ── s79c: COLLATE NOCASE ORDER BY ──

#[test]
fn test_conformance_collate_nocase_order_by_s79c() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE words(w TEXT COLLATE NOCASE)",
            "INSERT INTO words VALUES('banana'),('Apple'),('cherry'),('APRICOT'),('Blueberry')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT w FROM words ORDER BY w",
            "SELECT w FROM words ORDER BY w DESC",
            "SELECT DISTINCT w FROM words ORDER BY w",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "COLLATE NOCASE ORDER BY",
        );
    });
}

// ── s79d: COLLATE NOCASE in WHERE with aggregates ──

#[test]
fn test_conformance_collate_nocase_where_agg_s79d() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE items(name TEXT COLLATE NOCASE, qty INTEGER)",
            "INSERT INTO items VALUES('Apple',10),('apple',20),('APPLE',30),('Banana',5),('banana',15)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT name, SUM(qty) FROM items GROUP BY name ORDER BY name",
            "SELECT name, COUNT(*) FROM items GROUP BY name HAVING COUNT(*) > 1 ORDER BY name",
            "SELECT MIN(name), MAX(name), SUM(qty) FROM items",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "COLLATE NOCASE WHERE+AGG",
        );
    });
}

// ── s79e: Nested aggregate expressions ──

#[test]
fn test_conformance_nested_agg_expr_s79e() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE scores(student TEXT, score INTEGER)",
            "INSERT INTO scores VALUES('A',80),('A',90),('B',70),('B',85),('C',95)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT MAX(score) - MIN(score) FROM scores",
            "SELECT student, MAX(score) - MIN(score) AS range FROM scores GROUP BY student ORDER BY student",
            "SELECT COALESCE(MAX(score), 0) FROM scores WHERE score > 100",
            "SELECT COUNT(*) + SUM(score) FROM scores",
            "SELECT AVG(score) * 2, TOTAL(score) / COUNT(*) FROM scores",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "nested aggregate expressions",
        );
    });
}

// ── s79f: CASE with aggregates in SELECT ──

#[test]
fn test_conformance_case_with_agg_s79f() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE orders(status TEXT, amount REAL)",
            "INSERT INTO orders VALUES('shipped',100.0),('pending',50.0),('shipped',200.0),('cancelled',30.0),('pending',75.0)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT status, CASE WHEN SUM(amount) > 100 THEN 'high' ELSE 'low' END AS vol FROM orders GROUP BY status ORDER BY status",
            "SELECT CASE WHEN COUNT(*) > 3 THEN 'many' ELSE 'few' END FROM orders",
            "SELECT SUM(CASE WHEN status='shipped' THEN amount ELSE 0 END) AS shipped_total FROM orders",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "CASE with aggregates",
        );
    });
}

// ── s79g: Multi-table aggregate with JOIN ──

#[test]
fn test_conformance_multi_table_agg_join_s79g() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE departments(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE employees(id INTEGER PRIMARY KEY, dept_id INTEGER, salary REAL)",
            "INSERT INTO departments VALUES(1,'Engineering'),(2,'Sales'),(3,'HR')",
            "INSERT INTO employees VALUES(1,1,80000),(2,1,90000),(3,2,60000),(4,2,70000),(5,3,55000)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT d.name, COUNT(*), AVG(e.salary) FROM departments d JOIN employees e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name",
            "SELECT d.name, MIN(e.salary), MAX(e.salary) FROM departments d JOIN employees e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name",
            "SELECT d.name, SUM(e.salary) FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "multi-table aggregate JOIN",
        );
    });
}

// ── s79h: Subquery in HAVING clause ──

#[test]
fn test_conformance_subquery_in_having_s79h() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE sales(product TEXT, amount INTEGER)",
            "INSERT INTO sales VALUES('A',10),('A',20),('B',5),('B',100),('C',50)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING SUM(amount) > (SELECT AVG(amount) FROM sales) ORDER BY product",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "subquery in HAVING",
        );
    });
}

// ── s79i: Window functions with COLLATE NOCASE ──

#[test]
fn test_conformance_window_collate_nocase_s79i() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE people(name TEXT COLLATE NOCASE, age INTEGER)",
            "INSERT INTO people VALUES('alice',30),('BOB',25),('Charlie',35),('alice',28),('Alice',29)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT name, age, ROW_NUMBER() OVER (ORDER BY name) FROM people ORDER BY name, age",
            "SELECT name, age, RANK() OVER (ORDER BY name) FROM people ORDER BY name, age",
            "SELECT name, age, RANK() OVER (ORDER BY name), DENSE_RANK() OVER (ORDER BY name), PERCENT_RANK() OVER (ORDER BY name), CUME_DIST() OVER (ORDER BY name) FROM people ORDER BY name, age",
            "SELECT name, age, SUM(age) OVER (ORDER BY name RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM people ORDER BY name, age",
            "SELECT name, age, SUM(age) OVER (ORDER BY name, age RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM people ORDER BY name, age",
            "SELECT name, COUNT(*), RANK() OVER (ORDER BY name), DENSE_RANK() OVER (ORDER BY name), PERCENT_RANK() OVER (ORDER BY name), CUME_DIST() OVER (ORDER BY name) FROM people GROUP BY name ORDER BY name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "window with COLLATE NOCASE",
        );
    });
}

// ── s79j: COLLATE NOCASE in UNION/INTERSECT/EXCEPT ──

#[test]
fn test_conformance_collate_nocase_compound_s79j() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE t1(x TEXT COLLATE NOCASE)",
            "CREATE TABLE t2(x TEXT COLLATE NOCASE)",
            "INSERT INTO t1 VALUES('Apple'),('banana'),('Cherry')",
            "INSERT INTO t2 VALUES('apple'),('BANANA'),('Date')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT x FROM t1 UNION SELECT x FROM t2 ORDER BY x",
            "SELECT x FROM t1 INTERSECT SELECT x FROM t2 ORDER BY x",
            "SELECT x FROM t1 EXCEPT SELECT x FROM t2 ORDER BY x",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "COLLATE NOCASE compound",
        );
    });
}

// ── s79k: Aggregate with FILTER clause ──

#[test]
fn test_conformance_aggregate_filter_s79k() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE events(type TEXT, value INTEGER)",
            "INSERT INTO events VALUES('click',1),('view',2),('click',3),('purchase',10),('view',1),('click',5)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT COUNT(*) FILTER (WHERE type='click'), SUM(value) FILTER (WHERE type='click') FROM events",
            "SELECT type, COUNT(*), SUM(value) FILTER (WHERE value > 2) FROM events GROUP BY type ORDER BY type",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "aggregate FILTER",
        );
    });
}

// ── s79l: COLLATE NOCASE with IN operator ──

#[test]
fn test_conformance_collate_nocase_in_s79l() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE names(name TEXT COLLATE NOCASE)",
            "INSERT INTO names VALUES('Alice'),('BOB'),('Charlie'),('alice'),('bob')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT name FROM names WHERE name IN ('alice','charlie') ORDER BY rowid",
            "SELECT COUNT(*) FROM names WHERE name NOT IN ('alice')",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "COLLATE NOCASE IN",
        );
    });
}

// ── s79m: BETWEEN with mixed types ──

#[test]
fn test_conformance_between_mixed_types_s79m() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE vals(v)",
            "INSERT INTO vals VALUES(1),(2.5),('3'),(NULL),(10),(-5),(0)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT v FROM vals WHERE v BETWEEN 0 AND 5 ORDER BY v",
            "SELECT v FROM vals WHERE v NOT BETWEEN 1 AND 10 ORDER BY v",
            "SELECT COUNT(*) FROM vals WHERE v BETWEEN -10 AND 10",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "BETWEEN mixed types",
        );
    });
}

// ── s79n: Complex expression in ORDER BY ──

#[test]
fn test_conformance_complex_order_by_expr_s79n() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE t(a INTEGER, b TEXT)",
            "INSERT INTO t VALUES(3,'x'),(1,'z'),(2,'y'),(1,'a'),(3,'b')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT a, b FROM t ORDER BY a * -1, b",
            "SELECT a, b FROM t ORDER BY a + length(b), b DESC",
            "SELECT a, b FROM t ORDER BY CASE WHEN a > 2 THEN 0 ELSE 1 END, b",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "complex ORDER BY expressions",
        );
    });
}

// ── s79o: INSERT...RETURNING with expressions ──

#[test]
fn test_conformance_insert_returning_expr_s79o() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &["CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, n INTEGER DEFAULT 0)"];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "INSERT INTO t(val, n) VALUES('hello', 42) RETURNING id, upper(val), n * 2",
            "INSERT INTO t(val) VALUES('world') RETURNING id, val || '!', typeof(n)",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "INSERT RETURNING expressions",
        );
    });
}

// ── s79p: Multi-column UNIQUE conflict with INSERT OR ──

#[test]
fn test_conformance_multi_col_unique_conflict_s79p() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE pairs(a TEXT, b TEXT, val INTEGER, UNIQUE(a, b))",
            "INSERT INTO pairs VALUES('x','y',1),('x','z',2),('y','y',3)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "INSERT OR REPLACE INTO pairs VALUES('x','y',99)",
            "SELECT * FROM pairs ORDER BY a, b",
            "INSERT OR IGNORE INTO pairs VALUES('x','z',100)",
            "SELECT * FROM pairs ORDER BY a, b",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "multi-col UNIQUE conflict",
        );
    });
}

// ── s79q: Recursive CTE with aggregate ──

#[test]
fn test_conformance_recursive_cte_agg_s79q() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let queries = &[
            "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT SUM(x), AVG(x), MIN(x), MAX(x) FROM cnt",
            "WITH RECURSIVE fib(a, b) AS (VALUES(0, 1) UNION ALL SELECT b, a+b FROM fib WHERE b < 100) SELECT COUNT(*), MAX(a) FROM fib",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "recursive CTE aggregate",
        );
    });
}

// ── s79r: UPDATE...RETURNING ──

#[test]
fn test_conformance_update_returning_s79r() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE inventory(id INTEGER PRIMARY KEY, item TEXT, qty INTEGER)",
            "INSERT INTO inventory VALUES(1,'pen',100),(2,'pencil',200),(3,'eraser',50)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "UPDATE inventory SET qty = qty - 10 WHERE qty > 50 RETURNING id, item, qty",
            "SELECT * FROM inventory ORDER BY id",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "UPDATE RETURNING",
        );
    });
}

// ── s79s: DELETE...RETURNING with JOIN-like subquery ──

#[test]
fn test_conformance_delete_returning_s79s() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
            "INSERT INTO items VALUES(1,'a',1),(2,'b',0),(3,'c',0),(4,'d',1)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "DELETE FROM items WHERE active = 0 RETURNING id, name",
            "SELECT * FROM items ORDER BY id",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "DELETE RETURNING",
        );
    });
}

// ── s79t: COALESCE chain with NULL-heavy data ──

#[test]
fn test_conformance_coalesce_null_chain_s79t() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE data(a, b, c, d)",
            "INSERT INTO data VALUES(NULL, NULL, NULL, 'last')",
            "INSERT INTO data VALUES(NULL, 'second', NULL, NULL)",
            "INSERT INTO data VALUES('first', NULL, NULL, NULL)",
            "INSERT INTO data VALUES(NULL, NULL, NULL, NULL)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT COALESCE(a, b, c, d, 'none') FROM data ORDER BY rowid",
            "SELECT IIF(a IS NOT NULL, a, IIF(b IS NOT NULL, b, 'fallback')) FROM data ORDER BY rowid",
            "SELECT NULLIF(COALESCE(a, b), COALESCE(c, d)) FROM data ORDER BY rowid",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "COALESCE NULL chain",
        );
    });
}

// ── s79u: leftmost VALUES compound ORDER BY with empty result ──

#[test]
fn test_conformance_values_compound_empty_order_s79u() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let queries = &[
            "VALUES (1) EXCEPT SELECT 1 ORDER BY 1",
            "VALUES (1,2) EXCEPT SELECT 1,2 ORDER BY 2",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "VALUES compound ORDER BY empty result",
        );
    });
}
