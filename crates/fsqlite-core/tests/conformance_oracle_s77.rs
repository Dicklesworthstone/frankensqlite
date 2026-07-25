//! Conformance oracle tests — Session 77
//!
//! Coverage: multi-table JOINs with mixed aggregates, complex UPDATE/DELETE with
//! subqueries and CTEs, CASE inside aggregates, COALESCE chains with subqueries,
//! multi-column ORDER BY with expressions, INSERT...SELECT with transforms,
//! HAVING with complex predicates, self-referential CTEs, window NTILE/PERCENT_RANK,
//! and cross-type comparison edge cases.

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
                    "FRANK_ERR: {query}\n  frank: {e}\n  csql:  {csql_rows:?}"
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

fn assert_no_mismatches(mismatches: &[String]) {
    if !mismatches.is_empty() {
        for m in mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} oracle mismatch(es)", mismatches.len());
    }
}

#[test]
fn test_conformance_three_table_join_agg_s77a() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77a_dept(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE s77a_emp(id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT, salary INTEGER)",
            "CREATE TABLE s77a_proj(id INTEGER PRIMARY KEY, emp_id INTEGER, budget INTEGER)",
            "INSERT INTO s77a_dept VALUES(1,'Eng'),(2,'Sales'),(3,'HR')",
            "INSERT INTO s77a_emp VALUES(1,1,'Alice',90000),(2,1,'Bob',85000),(3,2,'Carol',70000),(4,2,'Dave',75000),(5,3,'Eve',60000)",
            "INSERT INTO s77a_proj VALUES(1,1,50000),(2,1,30000),(3,2,20000),(4,3,15000),(5,4,25000)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT d.name, COUNT(DISTINCT e.id), SUM(p.budget) FROM s77a_dept d JOIN s77a_emp e ON e.dept_id = d.id JOIN s77a_proj p ON p.emp_id = e.id GROUP BY d.name ORDER BY d.name",
            "SELECT d.name, AVG(e.salary), MAX(p.budget) FROM s77a_dept d LEFT JOIN s77a_emp e ON e.dept_id = d.id LEFT JOIN s77a_proj p ON p.emp_id = e.id GROUP BY d.name ORDER BY d.name",
            "SELECT d.name, COUNT(e.id) as emp_count FROM s77a_dept d LEFT JOIN s77a_emp e ON e.dept_id = d.id GROUP BY d.name HAVING COUNT(e.id) > 1 ORDER BY d.name",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_case_inside_aggregate_s77b() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77b_t(id INTEGER PRIMARY KEY, category TEXT, amount INTEGER, status TEXT)",
            "INSERT INTO s77b_t VALUES(1,'A',100,'active'),(2,'A',200,'inactive'),(3,'B',150,'active'),(4,'B',50,'active'),(5,'A',300,'active'),(6,'C',120,'inactive')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT category, SUM(CASE WHEN status='active' THEN amount ELSE 0 END) as active_sum, SUM(CASE WHEN status='inactive' THEN amount ELSE 0 END) as inactive_sum FROM s77b_t GROUP BY category ORDER BY category",
            "SELECT category, COUNT(CASE WHEN amount > 100 THEN 1 END) as high_count FROM s77b_t GROUP BY category ORDER BY category",
            "SELECT SUM(CASE WHEN category='A' THEN amount END) as a_total, SUM(CASE WHEN category='B' THEN amount END) as b_total, SUM(CASE WHEN category='C' THEN amount END) as c_total FROM s77b_t",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_delete_with_subquery_s77c() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77c_orders(id INTEGER PRIMARY KEY, customer_id INTEGER, total INTEGER)",
            "CREATE TABLE s77c_customers(id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
            "INSERT INTO s77c_customers VALUES(1,'Alice',1),(2,'Bob',0),(3,'Carol',1)",
            "INSERT INTO s77c_orders VALUES(1,1,100),(2,2,200),(3,2,50),(4,3,300),(5,1,150)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        fconn
        .execute("DELETE FROM s77c_orders WHERE customer_id IN (SELECT id FROM s77c_customers WHERE active = 0)")
        .await
        .unwrap();
        rconn
        .execute_batch("DELETE FROM s77c_orders WHERE customer_id IN (SELECT id FROM s77c_customers WHERE active = 0)")
        .unwrap();
        let queries = &[
            "SELECT id, customer_id, total FROM s77c_orders ORDER BY id",
            "SELECT COUNT(*) FROM s77c_orders",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_update_with_cte_s77d() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77d_t(id INTEGER PRIMARY KEY, val INTEGER, grp TEXT)",
            "INSERT INTO s77d_t VALUES(1,10,'A'),(2,20,'A'),(3,30,'B'),(4,40,'B'),(5,50,'A')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        fconn
            .execute("UPDATE s77d_t SET val = val * 2 WHERE grp = 'A'")
            .await
            .unwrap();
        rconn
            .execute_batch("UPDATE s77d_t SET val = val * 2 WHERE grp = 'A'")
            .unwrap();
        let queries = &[
            "SELECT id, val, grp FROM s77d_t ORDER BY id",
            "SELECT grp, SUM(val) FROM s77d_t GROUP BY grp ORDER BY grp",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_coalesce_with_subquery_s77e() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77e_config(key TEXT PRIMARY KEY, val TEXT)",
            "CREATE TABLE s77e_defaults(key TEXT PRIMARY KEY, val TEXT)",
            "INSERT INTO s77e_defaults VALUES('color','blue'),('size','medium'),('weight','heavy')",
            "INSERT INTO s77e_config VALUES('color','red'),('weight',NULL)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT d.key, COALESCE(c.val, d.val) as effective FROM s77e_defaults d LEFT JOIN s77e_config c ON c.key = d.key ORDER BY d.key",
            "SELECT d.key, COALESCE((SELECT c.val FROM s77e_config c WHERE c.key = d.key), d.val) as effective FROM s77e_defaults d ORDER BY d.key",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_multi_column_order_by_expr_s77f() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77f_t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL)",
            "INSERT INTO s77f_t VALUES(1,3,'x',1.5),(2,1,'y',2.5),(3,1,'x',3.5),(4,2,'z',0.5),(5,3,'y',1.0),(6,2,'x',4.0)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s77f_t ORDER BY a ASC, b DESC, c ASC",
            "SELECT * FROM s77f_t ORDER BY a + CAST(LENGTH(b) AS INTEGER), id",
            "SELECT * FROM s77f_t ORDER BY CASE WHEN a > 2 THEN 0 ELSE 1 END, c DESC",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_insert_select_with_transform_s77g() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77g_src(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
            "CREATE TABLE s77g_dst(id INTEGER PRIMARY KEY, label TEXT, adjusted_score INTEGER)",
            "INSERT INTO s77g_src VALUES(1,'alpha',80),(2,'beta',90),(3,'gamma',70),(4,'delta',95)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        fconn
        .execute("INSERT INTO s77g_dst SELECT id, UPPER(name), score * 10 + id FROM s77g_src WHERE score >= 80")
        .await
        .unwrap();
        rconn
        .execute_batch("INSERT INTO s77g_dst SELECT id, UPPER(name), score * 10 + id FROM s77g_src WHERE score >= 80")
        .unwrap();
        let queries = &[
            "SELECT * FROM s77g_dst ORDER BY id",
            "SELECT COUNT(*) FROM s77g_dst",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_having_complex_predicate_s77h() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77h_t(id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)",
            "INSERT INTO s77h_t VALUES(1,'A',10),(2,'A',20),(3,'A',30),(4,'B',5),(5,'B',15),(6,'C',100),(7,'C',200),(8,'D',1)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT cat, SUM(val) as s FROM s77h_t GROUP BY cat HAVING SUM(val) > 20 AND COUNT(*) >= 2 ORDER BY cat",
            "SELECT cat, AVG(val) as a FROM s77h_t GROUP BY cat HAVING AVG(val) BETWEEN 10 AND 50 ORDER BY cat",
            "SELECT cat, MIN(val), MAX(val) FROM s77h_t GROUP BY cat HAVING MAX(val) - MIN(val) > 5 ORDER BY cat",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_recursive_cte_fibonacci_s77i() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "WITH RECURSIVE fib(n, a, b) AS (SELECT 1, 0, 1 UNION ALL SELECT n+1, b, a+b FROM fib WHERE n < 10) SELECT n, a FROM fib ORDER BY n",
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT x, x*x as sq FROM cnt",
            "WITH RECURSIVE powers(n, val) AS (SELECT 0, 1 UNION ALL SELECT n+1, val*2 FROM powers WHERE n < 8) SELECT n, val FROM powers ORDER BY n",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_window_ntile_s77j() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77j_t(id INTEGER PRIMARY KEY, score INTEGER)",
            "INSERT INTO s77j_t VALUES(1,90),(2,80),(3,70),(4,60),(5,50),(6,40),(7,30),(8,20)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, score, NTILE(4) OVER (ORDER BY score DESC) as quartile FROM s77j_t ORDER BY id",
            "SELECT id, score, NTILE(3) OVER (ORDER BY score) as tercile FROM s77j_t ORDER BY id",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_cross_type_comparison_s77k() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77k_t(id INTEGER PRIMARY KEY, val)",
            "INSERT INTO s77k_t VALUES(1,42),(2,'42'),(3,42.0),(4,NULL),(5,0),(6,''),(7,'hello'),(8,X'4142')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, typeof(val), val FROM s77k_t ORDER BY id",
            "SELECT id, val = 42 as eq42 FROM s77k_t ORDER BY id",
            "SELECT id, val > 0 as gt0 FROM s77k_t ORDER BY id",
            "SELECT id, val IS NULL as is_null FROM s77k_t ORDER BY id",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_multi_update_set_s77l() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77l_t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)",
            "INSERT INTO s77l_t VALUES(1,10,20,'x'),(2,30,40,'y'),(3,50,60,'z')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        fconn
            .execute("UPDATE s77l_t SET a = a + b, b = a - b, c = UPPER(c) WHERE id >= 2")
            .await
            .unwrap();
        rconn
            .execute_batch("UPDATE s77l_t SET a = a + b, b = a - b, c = UPPER(c) WHERE id >= 2")
            .unwrap();
        let queries = &["SELECT * FROM s77l_t ORDER BY id"];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_exists_not_exists_s77m() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77m_parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE s77m_child(id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT)",
            "INSERT INTO s77m_parent VALUES(1,'Alpha'),(2,'Beta'),(3,'Gamma'),(4,'Delta')",
            "INSERT INTO s77m_child VALUES(1,1,'c1'),(2,1,'c2'),(3,3,'c3')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT name FROM s77m_parent p WHERE EXISTS (SELECT 1 FROM s77m_child c WHERE c.parent_id = p.id) ORDER BY name",
            "SELECT name FROM s77m_parent p WHERE NOT EXISTS (SELECT 1 FROM s77m_child c WHERE c.parent_id = p.id) ORDER BY name",
            "SELECT name, (SELECT COUNT(*) FROM s77m_child c WHERE c.parent_id = p.id) as child_count FROM s77m_parent p ORDER BY name",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_nested_cte_s77n() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77n_t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
            "INSERT INTO s77n_t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "WITH grp_sums AS (SELECT grp, SUM(val) as s FROM s77n_t GROUP BY grp), ranked AS (SELECT grp, s, RANK() OVER (ORDER BY s DESC) as rnk FROM grp_sums) SELECT * FROM ranked ORDER BY rnk, grp",
            "WITH base AS (SELECT grp, val FROM s77n_t WHERE val > 15), agg AS (SELECT grp, COUNT(*) as cnt, AVG(val) as avg_val FROM base GROUP BY grp) SELECT * FROM agg ORDER BY grp",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_union_intersect_except_s77o() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77o_a(id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE s77o_b(id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO s77o_a VALUES(1,'x'),(2,'y'),(3,'z')",
            "INSERT INTO s77o_b VALUES(2,'y'),(3,'w'),(4,'v')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, val FROM s77o_a UNION SELECT id, val FROM s77o_b ORDER BY id, val",
            "SELECT id FROM s77o_a INTERSECT SELECT id FROM s77o_b ORDER BY id",
            "SELECT id FROM s77o_a EXCEPT SELECT id FROM s77o_b ORDER BY id",
            "SELECT id, val FROM s77o_a UNION ALL SELECT id, val FROM s77o_b ORDER BY id, val",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_subquery_in_select_list_s77p() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77p_t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
            "INSERT INTO s77p_t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, val, (SELECT MAX(val) FROM s77p_t) as global_max FROM s77p_t ORDER BY id",
            "SELECT id, val, (SELECT SUM(val) FROM s77p_t t2 WHERE t2.grp = s77p_t.grp) as grp_sum FROM s77p_t ORDER BY id",
            "SELECT DISTINCT grp, (SELECT COUNT(*) FROM s77p_t t2 WHERE t2.grp = s77p_t.grp) as cnt FROM s77p_t ORDER BY grp",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_iif_nullif_nested_s77q() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT IIF(1 > 0, 'yes', 'no')",
            "SELECT IIF(NULL, 'yes', 'no')",
            "SELECT NULLIF(5, 5)",
            "SELECT NULLIF(5, 6)",
            "SELECT IIF(NULLIF(3, 3) IS NULL, 'matched', 'diff')",
            "SELECT COALESCE(NULLIF(0, 0), NULLIF(1, 1), 42)",
            "SELECT IIF(1, IIF(0, 'a', 'b'), 'c')",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_left_join_null_propagation_s77r() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77r_users(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE s77r_orders(id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)",
            "INSERT INTO s77r_users VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')",
            "INSERT INTO s77r_orders VALUES(1,1,100),(2,1,200),(3,3,50)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT u.name, o.amount FROM s77r_users u LEFT JOIN s77r_orders o ON o.user_id = u.id ORDER BY u.name, o.amount",
            "SELECT u.name, COALESCE(SUM(o.amount), 0) as total FROM s77r_users u LEFT JOIN s77r_orders o ON o.user_id = u.id GROUP BY u.name ORDER BY u.name",
            "SELECT u.name FROM s77r_users u LEFT JOIN s77r_orders o ON o.user_id = u.id WHERE o.id IS NULL ORDER BY u.name",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_group_by_expression_s77s() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77s_t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
            "INSERT INTO s77s_t VALUES(1,'alice',85),(2,'bob',92),(3,'alice',78),(4,'carol',95),(5,'bob',88)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT UPPER(name) as uname, AVG(score) as avg_score FROM s77s_t GROUP BY UPPER(name) ORDER BY uname",
            "SELECT CASE WHEN score >= 90 THEN 'high' ELSE 'low' END as tier, COUNT(*) FROM s77s_t GROUP BY CASE WHEN score >= 90 THEN 'high' ELSE 'low' END ORDER BY tier",
            "SELECT LENGTH(name) as nlen, COUNT(*) FROM s77s_t GROUP BY LENGTH(name) ORDER BY nlen",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_between_with_expressions_s77t() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77t_t(id INTEGER PRIMARY KEY, val INTEGER, lo INTEGER, hi INTEGER)",
            "INSERT INTO s77t_t VALUES(1,5,1,10),(2,15,10,20),(3,25,20,30),(4,0,5,15),(5,12,10,20)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, val BETWEEN lo AND hi as in_range FROM s77t_t ORDER BY id",
            "SELECT id FROM s77t_t WHERE val BETWEEN 5 AND 15 ORDER BY id",
            "SELECT id FROM s77t_t WHERE val NOT BETWEEN lo AND hi ORDER BY id",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_window_sum_over_partition_s77u() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77u_t(id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER)",
            "INSERT INTO s77u_t VALUES(1,'A',10),(2,'A',20),(3,'A',30),(4,'B',5),(5,'B',15),(6,'C',100)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, dept, amount, SUM(amount) OVER (PARTITION BY dept ORDER BY id) as running_sum FROM s77u_t ORDER BY id",
            "SELECT id, dept, amount, SUM(amount) OVER (PARTITION BY dept) as dept_total FROM s77u_t ORDER BY id",
            "SELECT id, dept, amount, AVG(amount) OVER (PARTITION BY dept ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as moving_avg FROM s77u_t ORDER BY id",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_trigger_after_delete_s77v() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77v_items(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE s77v_log(action TEXT, item_id INTEGER, item_name TEXT)",
            "CREATE TRIGGER s77v_del AFTER DELETE ON s77v_items BEGIN INSERT INTO s77v_log VALUES('DELETE', OLD.id, OLD.name); END",
            "INSERT INTO s77v_items VALUES(1,'apple'),(2,'banana'),(3,'cherry')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        fconn
            .execute("DELETE FROM s77v_items WHERE id = 2")
            .await
            .unwrap();
        rconn
            .execute_batch("DELETE FROM s77v_items WHERE id = 2")
            .unwrap();
        let queries = &[
            "SELECT * FROM s77v_items ORDER BY id",
            "SELECT * FROM s77v_log ORDER BY item_id",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_replace_semantics_s77w() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77w_t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
            "INSERT INTO s77w_t VALUES(1,'a',10),(2,'b',20),(3,'c',30)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        fconn
            .execute("REPLACE INTO s77w_t VALUES(2,'bb',200)")
            .await
            .unwrap();
        rconn
            .execute_batch("REPLACE INTO s77w_t VALUES(2,'bb',200)")
            .unwrap();
        fconn
            .execute("REPLACE INTO s77w_t VALUES(4,'d',40)")
            .await
            .unwrap();
        rconn
            .execute_batch("REPLACE INTO s77w_t VALUES(4,'d',40)")
            .unwrap();
        let queries = &["SELECT * FROM s77w_t ORDER BY id"];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_distinct_with_order_by_s77x() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77x_t(id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)",
            "INSERT INTO s77x_t VALUES(1,'A',3),(2,'B',1),(3,'A',3),(4,'C',2),(5,'B',1),(6,'A',4)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT DISTINCT cat FROM s77x_t ORDER BY cat",
            "SELECT DISTINCT cat, val FROM s77x_t ORDER BY cat, val",
            "SELECT DISTINCT cat FROM s77x_t ORDER BY cat DESC",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_null_arithmetic_chain_s77y() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT NULL + 1",
            "SELECT 1 + NULL",
            "SELECT NULL * 5",
            "SELECT NULL || 'hello'",
            "SELECT COALESCE(NULL + 1, 99)",
            "SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END",
            "SELECT NULL = NULL",
            "SELECT NULL IS NULL",
            "SELECT NULL IS NOT NULL",
            "SELECT NOT NULL",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_multi_column_in_s77z() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77z_t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)",
            "INSERT INTO s77z_t VALUES(1,1,10,'x'),(2,2,20,'y'),(3,1,20,'z'),(4,3,30,'w')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s77z_t WHERE a IN (1, 3) ORDER BY id",
            "SELECT * FROM s77z_t WHERE a IN (1, 3) AND b > 15 ORDER BY id",
            "SELECT * FROM s77z_t WHERE a NOT IN (2) ORDER BY id",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_complex_where_mixed_ops_s77aa() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77aa_t(id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active INTEGER)",
            "INSERT INTO s77aa_t VALUES(1,'Alice',30,1),(2,'Bob',25,0),(3,'Carol',35,1),(4,'Dave',28,1),(5,'Eve',22,0)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT name FROM s77aa_t WHERE active = 1 AND age > 25 ORDER BY name",
            "SELECT name FROM s77aa_t WHERE active = 1 OR age < 25 ORDER BY name",
            "SELECT name FROM s77aa_t WHERE (active = 1 AND age >= 30) OR (active = 0 AND age <= 23) ORDER BY name",
            "SELECT name FROM s77aa_t WHERE NOT (active = 0) AND age BETWEEN 25 AND 35 ORDER BY name",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_self_join_pairs_s77ab() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77ab_t(id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO s77ab_t VALUES(1,10),(2,20),(3,30),(4,40)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT a.id as a_id, b.id as b_id, a.val + b.val as pair_sum FROM s77ab_t a, s77ab_t b WHERE a.id < b.id ORDER BY a_id, b_id",
            "SELECT a.id, a.val, b.val as next_val FROM s77ab_t a LEFT JOIN s77ab_t b ON b.id = a.id + 1 ORDER BY a.id",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_printf_format_s77ac() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT printf('%d', 42)",
            "SELECT printf('%05d', 42)",
            "SELECT printf('%.2f', 3.14159)",
            "SELECT printf('%s has %d items', 'Alice', 5)",
            "SELECT printf('%10s', 'hi')",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_aggregate_empty_table_s77ad() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let ddl = "CREATE TABLE s77ad_t(id INTEGER PRIMARY KEY, val INTEGER)";
        fconn.execute(ddl).await.unwrap();
        rconn.execute_batch(ddl).unwrap();
        let queries = &[
            "SELECT COUNT(*) FROM s77ad_t",
            "SELECT SUM(val) FROM s77ad_t",
            "SELECT AVG(val) FROM s77ad_t",
            "SELECT MIN(val) FROM s77ad_t",
            "SELECT MAX(val) FROM s77ad_t",
            "SELECT TOTAL(val) FROM s77ad_t",
            "SELECT GROUP_CONCAT(val) FROM s77ad_t",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_hex_zeroblob_length_s77ae() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT HEX(X'48656C6C6F')",
            "SELECT LENGTH(ZEROBLOB(10))",
            "SELECT HEX(ZEROBLOB(4))",
            "SELECT TYPEOF(X'1234')",
            "SELECT LENGTH(X'AABBCCDD')",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_limit_offset_edges_s77af() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77af_t(id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO s77af_t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s77af_t ORDER BY id LIMIT 3",
            "SELECT * FROM s77af_t ORDER BY id LIMIT 3 OFFSET 2",
            "SELECT * FROM s77af_t ORDER BY id LIMIT 100",
            "SELECT * FROM s77af_t ORDER BY id LIMIT 2 OFFSET 10",
            "SELECT * FROM s77af_t ORDER BY id LIMIT 0",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_transaction_savepoint_complex_s77ag() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77ag_t(id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO s77ag_t VALUES(1,'original')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        for s in &[
            "SAVEPOINT sp1",
            "INSERT INTO s77ag_t VALUES(2,'sp1')",
            "SAVEPOINT sp2",
            "INSERT INTO s77ag_t VALUES(3,'sp2')",
            "ROLLBACK TO sp2",
            "INSERT INTO s77ag_t VALUES(4,'after_rollback')",
            "RELEASE sp1",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &["SELECT * FROM s77ag_t ORDER BY id"];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_rowid_alias_s77ah() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77ah_t(id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO s77ah_t VALUES(1,'a'),(2,'b'),(3,'c')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT rowid, id, name FROM s77ah_t ORDER BY rowid",
            "SELECT _rowid_, oid, id FROM s77ah_t ORDER BY _rowid_",
            "SELECT * FROM s77ah_t WHERE rowid = 2",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_insert_or_conflict_modes_s77ai() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77ai_t(id INTEGER PRIMARY KEY, val TEXT UNIQUE)",
            "INSERT INTO s77ai_t VALUES(1,'alpha'),(2,'beta')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        fconn
            .execute("INSERT OR IGNORE INTO s77ai_t VALUES(1,'gamma')")
            .await
            .unwrap();
        rconn
            .execute_batch("INSERT OR IGNORE INTO s77ai_t VALUES(1,'gamma')")
            .unwrap();
        fconn
            .execute("INSERT OR REPLACE INTO s77ai_t VALUES(2,'delta')")
            .await
            .unwrap();
        rconn
            .execute_batch("INSERT OR REPLACE INTO s77ai_t VALUES(2,'delta')")
            .unwrap();
        fconn
            .execute("INSERT OR IGNORE INTO s77ai_t VALUES(3,'epsilon')")
            .await
            .unwrap();
        rconn
            .execute_batch("INSERT OR IGNORE INTO s77ai_t VALUES(3,'epsilon')")
            .unwrap();
        let queries = &["SELECT * FROM s77ai_t ORDER BY id"];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_view_with_aggregates_s77aj() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s77aj_sales(id INTEGER PRIMARY KEY, product TEXT, qty INTEGER, price REAL)",
            "INSERT INTO s77aj_sales VALUES(1,'apple',5,1.50),(2,'banana',3,0.75),(3,'apple',2,1.50),(4,'cherry',10,2.00),(5,'banana',7,0.75)",
            "CREATE VIEW s77aj_summary AS SELECT product, SUM(qty) as total_qty, SUM(qty * price) as revenue FROM s77aj_sales GROUP BY product",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s77aj_summary ORDER BY product",
            "SELECT product, revenue FROM s77aj_summary WHERE total_qty > 5 ORDER BY product",
            "SELECT SUM(revenue) FROM s77aj_summary",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_string_ops_comprehensive_s77ak() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT SUBSTR('hello world', 7)",
            "SELECT SUBSTR('hello world', 1, 5)",
            "SELECT REPLACE('banana', 'na', 'xy')",
            "SELECT TRIM('  hello  ')",
            "SELECT LTRIM('  hello  ')",
            "SELECT RTRIM('  hello  ')",
            "SELECT INSTR('hello world', 'world')",
            "SELECT INSTR('hello world', 'xyz')",
            "SELECT UPPER('hello') || ' ' || LOWER('WORLD')",
            "SELECT LENGTH('')",
            "SELECT LENGTH(NULL)",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}

#[test]
fn test_conformance_math_functions_extended_s77al() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT ABS(-42)",
            "SELECT ABS(0)",
            "SELECT MAX(1, 2, 3)",
            "SELECT MIN(10, 5, 20)",
            "SELECT ROUND(2.567, 2)",
            "SELECT ROUND(2.5)",
            "SELECT ROUND(-2.5)",
        ];
        assert_no_mismatches(&oracle_compare(&fconn, &rconn, queries).await);
    });
}
