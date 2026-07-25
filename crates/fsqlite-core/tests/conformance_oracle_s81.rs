//! Conformance oracle tests — Session 81 (cc4 JOIN + planner + edge cases)
//!
//! Targeted probes after PLANNER-3 join-order permutation landed:
//! multi-table JOINs, hash-join correctness, LEFT JOIN preservation,
//! self-joins, subquery-derived tables, and complex predicate pushdown.

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

// ── s81a: Three-table INNER JOIN ──

#[test]
fn test_conformance_three_table_join_s81a() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL)",
            "CREATE TABLE items(id INTEGER PRIMARY KEY, order_id INTEGER, product TEXT)",
            "INSERT INTO customers VALUES(1,'Alice'),(2,'Bob'),(3,'Charlie')",
            "INSERT INTO orders VALUES(10,1,100.0),(20,2,200.0),(30,1,50.0)",
            "INSERT INTO items VALUES(100,10,'Widget'),(101,10,'Gadget'),(102,20,'Doohickey'),(103,30,'Thingamajig')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT c.name, o.total, i.product FROM customers c JOIN orders o ON c.id = o.customer_id JOIN items i ON o.id = i.order_id ORDER BY c.name, i.product",
            "SELECT c.name, COUNT(i.id), SUM(o.total) FROM customers c JOIN orders o ON c.id = o.customer_id JOIN items i ON o.id = i.order_id GROUP BY c.name ORDER BY c.name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "three-table JOIN",
        );
    });
}

// ── s81b: LEFT JOIN preserving NULLs ──

#[test]
fn test_conformance_left_join_nulls_s81b() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE departments(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE employees(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            "INSERT INTO departments VALUES(1,'Engineering'),(2,'Sales'),(3,'Marketing')",
            "INSERT INTO employees VALUES(10,'Alice',1),(20,'Bob',1),(30,'Charlie',2)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT d.name, e.name FROM departments d LEFT JOIN employees e ON d.id = e.dept_id ORDER BY d.name, e.name",
            "SELECT d.name, COUNT(e.id) FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name",
            "SELECT d.name FROM departments d LEFT JOIN employees e ON d.id = e.dept_id WHERE e.id IS NULL",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "LEFT JOIN NULLs",
        );
    });
}

// ── s81c: Self-join for hierarchical data ──

#[test]
fn test_conformance_self_join_hierarchy_s81c() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)",
            "INSERT INTO emp VALUES(1,'CEO',NULL),(2,'VP1',1),(3,'VP2',1),(4,'Mgr1',2),(5,'Dev1',4),(6,'Dev2',4)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT e.name, m.name AS manager FROM emp e LEFT JOIN emp m ON e.manager_id = m.id ORDER BY e.name",
            "SELECT m.name, COUNT(e.id) AS reports FROM emp e JOIN emp m ON e.manager_id = m.id GROUP BY m.name ORDER BY reports DESC, m.name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "self-join hierarchy",
        );
    });
}

// ── s81d: JOIN with aggregate and HAVING ──

#[test]
fn test_conformance_join_agg_having_s81d() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE books(id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT, sales INTEGER)",
            "INSERT INTO authors VALUES(1,'Tolkien'),(2,'Rowling'),(3,'Martin')",
            "INSERT INTO books VALUES(10,1,'LOTR',1000),(11,1,'Hobbit',800),(20,2,'HP1',5000),(21,2,'HP2',4500),(30,3,'GOT',3000)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT a.name, SUM(b.sales) AS total FROM authors a JOIN books b ON a.id = b.author_id GROUP BY a.name HAVING SUM(b.sales) > 2000 ORDER BY total DESC",
            "SELECT a.name, COUNT(b.id), MAX(b.sales) FROM authors a JOIN books b ON a.id = b.author_id GROUP BY a.name ORDER BY a.name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "JOIN agg HAVING",
        );
    });
}

// ── s81e: Derived table (subquery in FROM) ──

#[test]
fn test_conformance_derived_table_s81e() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE sales(region TEXT, month INTEGER, amount REAL)",
            "INSERT INTO sales VALUES('East',1,100),('East',2,150),('West',1,200),('West',2,250),('East',3,120)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT region, total FROM (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) ORDER BY region",
            "SELECT * FROM (SELECT region, COUNT(*) AS cnt, AVG(amount) AS avg_amt FROM sales GROUP BY region) WHERE cnt > 2",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "derived table",
        );
    });
}

// ── s81f: CROSS JOIN ──

#[test]
fn test_conformance_cross_join_s81f() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE colors(c TEXT)",
            "CREATE TABLE sizes(s TEXT)",
            "INSERT INTO colors VALUES('Red'),('Blue'),('Green')",
            "INSERT INTO sizes VALUES('S'),('M'),('L')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT c, s FROM colors CROSS JOIN sizes ORDER BY c, s",
            "SELECT COUNT(*) FROM colors CROSS JOIN sizes",
        ];

        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await, "CROSS JOIN");
    });
}

// ── s81g: JOIN with OR predicates ──

#[test]
fn test_conformance_join_or_predicate_s81g() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "CREATE TABLE t2(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)",
            "INSERT INTO t1 VALUES(1,10,20),(2,30,40),(3,50,60)",
            "INSERT INTO t2 VALUES(1,10,99),(2,99,40),(3,50,60)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT t1.id, t2.id FROM t1, t2 WHERE t1.a = t2.x OR t1.b = t2.y ORDER BY t1.id, t2.id",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "JOIN OR predicate",
        );
    });
}

// ── s81h: Multi-table JOIN with aggregate window ──

#[test]
fn test_conformance_join_window_s81h() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE products(id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT, price REAL)",
            "INSERT INTO categories VALUES(1,'Electronics'),(2,'Books')",
            "INSERT INTO products VALUES(10,1,'Phone',999),(11,1,'Laptop',1499),(12,1,'Tablet',599),(20,2,'Novel',15),(21,2,'Textbook',80)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT c.name, p.name, p.price, ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY p.price DESC) AS rank FROM categories c JOIN products p ON c.id = p.cat_id ORDER BY c.name, rank",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "JOIN window",
        );
    });
}

// ── s81i: CTE with JOIN ──

#[test]
fn test_conformance_cte_join_s81i() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer TEXT, amount REAL, status TEXT)",
            "INSERT INTO orders VALUES(1,'Alice',100,'shipped'),(2,'Bob',200,'pending'),(3,'Alice',50,'shipped'),(4,'Charlie',300,'pending'),(5,'Bob',150,'shipped')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "WITH shipped AS (SELECT customer, SUM(amount) AS total FROM orders WHERE status='shipped' GROUP BY customer), pending AS (SELECT customer, SUM(amount) AS total FROM orders WHERE status='pending' GROUP BY customer) SELECT COALESCE(s.customer, p.customer) AS customer, COALESCE(s.total, 0) AS shipped_total, COALESCE(p.total, 0) AS pending_total FROM shipped s LEFT JOIN pending p ON s.customer = p.customer UNION SELECT p.customer, 0, p.total FROM pending p WHERE p.customer NOT IN (SELECT customer FROM shipped) ORDER BY customer",
        ];

        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await, "CTE JOIN");
    });
}

// ── s81j: JOIN with DISTINCT ──

#[test]
fn test_conformance_join_distinct_s81j() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE tags(item_id INTEGER, tag TEXT)",
            "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO items VALUES(1,'A'),(2,'B'),(3,'C')",
            "INSERT INTO tags VALUES(1,'x'),(1,'y'),(2,'x'),(2,'x'),(3,'z')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT DISTINCT i.name, t.tag FROM items i JOIN tags t ON i.id = t.item_id ORDER BY i.name, t.tag",
            "SELECT i.name, COUNT(DISTINCT t.tag) FROM items i JOIN tags t ON i.id = t.item_id GROUP BY i.name ORDER BY i.name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "JOIN DISTINCT",
        );
    });
}

// ── s81k: Correlated subquery in SELECT list with JOIN ──

#[test]
fn test_conformance_correlated_subquery_select_s81k() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE dept(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)",
            "INSERT INTO dept VALUES(1,'Eng'),(2,'Sales')",
            "INSERT INTO emp VALUES(10,'Alice',1,100000),(11,'Bob',1,90000),(20,'Charlie',2,80000),(21,'Diana',2,85000)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT e.name, e.salary, (SELECT AVG(e2.salary) FROM emp e2 WHERE e2.dept_id = e.dept_id) AS dept_avg FROM emp e ORDER BY e.name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "correlated subquery SELECT",
        );
    });
}

// ── s81l: Multi-column JOIN key ──

#[test]
fn test_conformance_multi_col_join_key_s81l() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE t1(a INTEGER, b INTEGER, val TEXT)",
            "CREATE TABLE t2(x INTEGER, y INTEGER, info TEXT)",
            "INSERT INTO t1 VALUES(1,10,'one'),(2,20,'two'),(3,30,'three'),(1,20,'one-twenty')",
            "INSERT INTO t2 VALUES(1,10,'match-a'),(2,20,'match-b'),(1,20,'match-c'),(4,40,'no-match')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT t1.val, t2.info FROM t1 JOIN t2 ON t1.a = t2.x AND t1.b = t2.y ORDER BY t1.val",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "multi-col JOIN key",
        );
    });
}

// ── s81m: JOIN with CASE in SELECT ──

#[test]
fn test_conformance_join_case_select_s81m() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE students(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE scores(student_id INTEGER, subject TEXT, score INTEGER)",
            "INSERT INTO students VALUES(1,'Alice'),(2,'Bob'),(3,'Charlie')",
            "INSERT INTO scores VALUES(1,'Math',95),(1,'English',88),(2,'Math',72),(2,'English',91),(3,'Math',60),(3,'English',55)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT s.name, sc.subject, sc.score, CASE WHEN sc.score >= 90 THEN 'A' WHEN sc.score >= 80 THEN 'B' WHEN sc.score >= 70 THEN 'C' ELSE 'F' END AS grade FROM students s JOIN scores sc ON s.id = sc.student_id ORDER BY s.name, sc.subject",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "JOIN CASE SELECT",
        );
    });
}

// ── s81n: LEFT JOIN with COALESCE ──

#[test]
fn test_conformance_left_join_coalesce_s81n() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE profiles(user_id INTEGER, bio TEXT)",
            "INSERT INTO users VALUES(1,'Alice'),(2,'Bob'),(3,'Charlie')",
            "INSERT INTO profiles VALUES(1,'Engineer'),(3,'Designer')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT u.name, COALESCE(p.bio, 'No profile') AS bio FROM users u LEFT JOIN profiles p ON u.id = p.user_id ORDER BY u.name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "LEFT JOIN COALESCE",
        );
    });
}

// ── s81o: Four-table JOIN chain ──

#[test]
fn test_conformance_four_table_join_s81o() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER, val TEXT)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, b_id INTEGER, val TEXT)",
            "CREATE TABLE d(id INTEGER PRIMARY KEY, c_id INTEGER, val TEXT)",
            "INSERT INTO a VALUES(1,'a1'),(2,'a2')",
            "INSERT INTO b VALUES(10,1,'b1'),(20,2,'b2'),(30,1,'b3')",
            "INSERT INTO c VALUES(100,10,'c1'),(200,20,'c2'),(300,30,'c3')",
            "INSERT INTO d VALUES(1000,100,'d1'),(2000,200,'d2')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT a.val, b.val, c.val, d.val FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id JOIN d ON c.id = d.c_id ORDER BY a.val, b.val, c.val, d.val",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "four-table JOIN",
        );
    });
}

// ── s81p: JOIN with UNION ──

#[test]
fn test_conformance_join_union_s81p() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE domestic(id INTEGER PRIMARY KEY, name TEXT, revenue REAL)",
            "CREATE TABLE intl(id INTEGER PRIMARY KEY, name TEXT, revenue REAL)",
            "INSERT INTO domestic VALUES(1,'NYC',1000),(2,'LA',800)",
            "INSERT INTO intl VALUES(1,'London',1200),(2,'Tokyo',900)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT name, revenue, 'domestic' AS source FROM domestic UNION ALL SELECT name, revenue, 'international' FROM intl ORDER BY revenue DESC",
        ];

        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await, "JOIN UNION");
    });
}

// ── s81q: Subquery in JOIN ON clause ──

#[test]
fn test_conformance_subquery_join_on_s81q() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE TABLE t2(id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO t1 VALUES(1,10),(2,20),(3,30)",
            "INSERT INTO t2 VALUES(1,5),(2,15),(3,25)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT t1.id, t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.val > (SELECT AVG(val) FROM t1) ORDER BY t1.id",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "subquery JOIN ON",
        );
    });
}

// ── s81r: JOIN preserving ORDER BY on multiple columns ──

#[test]
fn test_conformance_join_order_by_multi_s81r() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, p_id INTEGER, label TEXT, priority INTEGER)",
            "INSERT INTO p VALUES(1,'Parent1'),(2,'Parent2')",
            "INSERT INTO c VALUES(10,1,'C-high',1),(11,1,'A-low',3),(12,1,'B-med',2),(20,2,'X-high',1),(21,2,'Y-low',2)",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT p.name, c.label, c.priority FROM p JOIN c ON p.id = c.p_id ORDER BY p.name, c.priority",
            "SELECT p.name, c.label FROM p JOIN c ON p.id = c.p_id ORDER BY p.name DESC, c.label ASC",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "JOIN ORDER BY multi",
        );
    });
}

// ── s81s: JOIN with LIMIT/OFFSET ──

#[test]
fn test_conformance_join_limit_offset_s81s() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE t2(t1_id INTEGER, extra TEXT)",
            "INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')",
            "INSERT INTO t2 VALUES(1,'x1'),(2,'x2'),(3,'x3'),(4,'x4'),(5,'x5')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT t1.val, t2.extra FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id LIMIT 3",
            "SELECT t1.val, t2.extra FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id LIMIT 2 OFFSET 2",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "JOIN LIMIT OFFSET",
        );
    });
}

// ── s81t: JOIN with GROUP_CONCAT ──

#[test]
fn test_conformance_join_group_concat_s81t() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let setup = &[
            "CREATE TABLE teams(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE members(id INTEGER PRIMARY KEY, team_id INTEGER, name TEXT)",
            "INSERT INTO teams VALUES(1,'Alpha'),(2,'Beta')",
            "INSERT INTO members VALUES(10,1,'Alice'),(11,1,'Bob'),(12,1,'Charlie'),(20,2,'Diana'),(21,2,'Eve')",
        ];
        for s in setup {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }

        let queries = &[
            "SELECT t.name, GROUP_CONCAT(m.name, ', ') FROM teams t JOIN members m ON t.id = m.team_id GROUP BY t.name ORDER BY t.name",
        ];

        assert_no_mismatches(
            &oracle_compare(&fconn, &rconn, queries).await,
            "JOIN GROUP_CONCAT",
        );
    });
}
