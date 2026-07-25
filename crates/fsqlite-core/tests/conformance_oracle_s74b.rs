//! Conformance oracle tests — Session 74b
//!
//! GROUP BY + window function coverage (leveraging the execute_group_by_window_select fix),
//! complex aggregates, CASE expressions, recursive CTEs, NULL functions, WHERE patterns,
//! string functions, and UPSERT conflict handling.

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

#[test]
fn test_oracle_compare_flags_dual_error_cases() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();

        let mismatches =
            oracle_compare(&fconn, &rconn, &["SELECT * FROM missing_oracle_s74b_table"]).await;
        assert_eq!(mismatches.len(), 1);
        assert!(mismatches[0].contains("BOTH_ERROR"));
    });
}

#[test]
fn test_conformance_group_by_rank_window_s74aa() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74aa_t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
            "INSERT INTO s74aa_t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'C',15),(5,'C',25),(6,'D',5)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT grp, SUM(val), RANK() OVER (ORDER BY SUM(val) DESC) as rnk FROM s74aa_t GROUP BY grp ORDER BY rnk, grp",
            "SELECT grp, AVG(val), DENSE_RANK() OVER (ORDER BY AVG(val)) as dr FROM s74aa_t GROUP BY grp ORDER BY dr, grp",
            "SELECT grp, MAX(val), ROW_NUMBER() OVER (ORDER BY MAX(val) DESC, grp) as rn FROM s74aa_t GROUP BY grp ORDER BY rn",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74aa mismatches", mismatches.len());
        }
    });
}

#[test]
fn test_conformance_case_complex_s74ab() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ab_t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER, status TEXT)",
            "INSERT INTO s74ab_t VALUES(1,'Alice',95,'active'),(2,'Bob',60,'inactive'),(3,'Carol',80,'active'),(4,'Dave',45,'active'),(5,'Eve',70,'inactive')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END as grade FROM s74ab_t ORDER BY name",
            "SELECT status, COUNT(*), AVG(score), CASE WHEN AVG(score) > 70 THEN 'good' ELSE 'needs work' END as assessment FROM s74ab_t GROUP BY status ORDER BY status",
            "SELECT name, IIF(score > 75, 'pass', 'fail') as result FROM s74ab_t ORDER BY name",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ab mismatches", mismatches.len());
        }
    });
}

#[test]
fn test_conformance_recursive_cte_agg_s74ac() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ac_tree(id INTEGER PRIMARY KEY, pid INTEGER, name TEXT, val INTEGER)",
            "INSERT INTO s74ac_tree VALUES(1,NULL,'root',100),(2,1,'c1',50),(3,1,'c2',30),(4,2,'gc1',20),(5,2,'gc2',10),(6,3,'gc3',15)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "WITH RECURSIVE tp AS (SELECT id, name, pid, 0 as depth FROM s74ac_tree WHERE pid IS NULL UNION ALL SELECT t.id, t.name, t.pid, tp.depth+1 FROM s74ac_tree t JOIN tp ON t.pid=tp.id) SELECT name, depth FROM tp ORDER BY depth, name",
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5) SELECT n, n*n as sq FROM c",
            "SELECT pid, COUNT(*) as ch, SUM(val) as tot FROM s74ac_tree WHERE pid IS NOT NULL GROUP BY pid ORDER BY pid",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ac mismatches", mismatches.len());
        }
    });
}

#[test]
fn test_conformance_null_functions_s74ad() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ad_t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)",
            "INSERT INTO s74ad_t VALUES(1,10,NULL,'x'),(2,NULL,20,'y'),(3,30,30,NULL),(4,NULL,NULL,'z')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, COALESCE(a, b, 0) as val FROM s74ad_t ORDER BY id",
            "SELECT id, NULLIF(a, b) as diff FROM s74ad_t ORDER BY id",
            "SELECT id, IIF(a IS NOT NULL, a, IIF(b IS NOT NULL, b, -1)) as resolved FROM s74ad_t ORDER BY id",
            "SELECT id, COALESCE(c, 'missing') as lbl, COALESCE(a, 0) + COALESCE(b, 0) as tot FROM s74ad_t ORDER BY id",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ad mismatches", mismatches.len());
        }
    });
}

#[test]
fn test_conformance_complex_where_s74af() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74af_t(id INTEGER PRIMARY KEY, x INTEGER, y REAL, z TEXT)",
            "INSERT INTO s74af_t VALUES(1,10,1.5,'alpha'),(2,20,2.5,'beta'),(3,30,3.5,'gamma'),(4,40,4.5,'delta'),(5,50,5.5,'epsilon'),(6,NULL,NULL,NULL)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74af_t WHERE x > 15 AND y < 5.0 ORDER BY id",
            "SELECT * FROM s74af_t WHERE x BETWEEN 20 AND 40 ORDER BY id",
            "SELECT * FROM s74af_t WHERE z LIKE '%a%' ORDER BY id",
            "SELECT * FROM s74af_t WHERE x IS NULL OR y IS NULL ORDER BY id",
            "SELECT * FROM s74af_t WHERE x IN (10, 30, 50) ORDER BY id",
            "SELECT * FROM s74af_t WHERE NOT (x > 25) AND x IS NOT NULL ORDER BY id",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74af mismatches", mismatches.len());
        }
    });
}

#[test]
fn test_conformance_aggregate_edges_s74ag() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ag_t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
            "INSERT INTO s74ag_t VALUES(1,'A',10),(2,'A',10),(3,'A',20),(4,'B',NULL),(5,'B',30),(6,'C',40),(7,'C',40),(8,'C',40)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT grp, COUNT(val), COUNT(DISTINCT val), SUM(val), SUM(DISTINCT val) FROM s74ag_t GROUP BY grp ORDER BY grp",
            "SELECT grp, AVG(val), MIN(val), MAX(val), total(val) FROM s74ag_t GROUP BY grp ORDER BY grp",
            "SELECT COUNT(*), COUNT(val), SUM(val), AVG(val) FROM s74ag_t",
            "SELECT grp, GROUP_CONCAT(val, ',') FROM s74ag_t GROUP BY grp ORDER BY grp",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ag mismatches", mismatches.len());
        }
    });
}

#[test]
fn test_conformance_string_functions_s74ai() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT length('hello'), length('')",
            "SELECT upper('hello'), lower('WORLD')",
            "SELECT substr('abcdef', 2, 3)",
            "SELECT replace('hello world', 'world', 'rust')",
            "SELECT trim('  hello  '), ltrim('  hello'), rtrim('hello  ')",
            "SELECT instr('hello world', 'world')",
            "SELECT 'hello' || ' ' || 'world'",
            "SELECT typeof(NULL), typeof(42), typeof(3.14), typeof('text'), typeof(X'00')",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ai mismatches", mismatches.len());
        }
    });
}

#[test]
fn test_conformance_upsert_conflict_s74aj() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74aj_t(id INTEGER PRIMARY KEY, name TEXT UNIQUE, val INTEGER DEFAULT 0)",
            "INSERT INTO s74aj_t VALUES(1,'Alice',10),(2,'Bob',20)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        for s in &[
            "INSERT OR REPLACE INTO s74aj_t VALUES(1,'Alice',100)",
            "INSERT OR IGNORE INTO s74aj_t VALUES(2,'Bob',200)",
            "INSERT OR REPLACE INTO s74aj_t VALUES(3,'Carol',30)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74aj_t ORDER BY id",
            "SELECT COUNT(*) FROM s74aj_t",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74aj mismatches", mismatches.len());
        }
    });
}

// ── s74ak: Multi-table JOIN with aggregate + HAVING ──

#[test]
fn test_conformance_multi_join_agg_having_s74ak() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ak_dept(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE s74ak_emp(id INTEGER PRIMARY KEY, dept_id INTEGER, salary REAL, FOREIGN KEY(dept_id) REFERENCES s74ak_dept(id))",
            "INSERT INTO s74ak_dept VALUES(1,'Eng'),(2,'Sales'),(3,'HR')",
            "INSERT INTO s74ak_emp VALUES(1,1,90000),(2,1,85000),(3,1,95000),(4,2,70000),(5,2,65000),(6,3,60000)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT d.name, COUNT(e.id) AS cnt, AVG(e.salary) AS avg_sal FROM s74ak_dept d LEFT JOIN s74ak_emp e ON d.id = e.dept_id GROUP BY d.name HAVING COUNT(e.id) > 1 ORDER BY d.name",
            "SELECT d.name, SUM(e.salary) FROM s74ak_dept d JOIN s74ak_emp e ON d.id = e.dept_id GROUP BY d.name ORDER BY SUM(e.salary) DESC",
            "SELECT d.name, MIN(e.salary), MAX(e.salary) FROM s74ak_dept d JOIN s74ak_emp e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ak mismatches", mismatches.len());
        }
    });
}

// ── s74al: Window frame ROWS BETWEEN ──

#[test]
fn test_conformance_window_rows_between_s74al() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74al_t(id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO s74al_t VALUES(1,10),(2,20),(3,30),(4,40),(5,50)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, val, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_sum FROM s74al_t ORDER BY id",
            "SELECT id, val, AVG(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg FROM s74al_t ORDER BY id",
            "SELECT id, val, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt FROM s74al_t ORDER BY id",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74al mismatches", mismatches.len());
        }
    });
}

// ── s74am: Savepoint nested rollback semantics ──

#[test]
fn test_conformance_savepoint_nested_rollback_s74am() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74am_t(id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO s74am_t VALUES(1,'orig')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        for s in &[
            "SAVEPOINT sp1",
            "INSERT INTO s74am_t VALUES(2,'sp1')",
            "SAVEPOINT sp2",
            "INSERT INTO s74am_t VALUES(3,'sp2')",
            "ROLLBACK TO sp2",
            "INSERT INTO s74am_t VALUES(4,'after_rb')",
            "RELEASE sp1",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &["SELECT * FROM s74am_t ORDER BY id"];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74am mismatches", mismatches.len());
        }
    });
}

// ── s74an: FK cascade DELETE + SET NULL chain ──

#[test]
fn test_conformance_fk_cascade_chain_s74an() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE s74an_a(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE s74an_b(id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES s74an_a(id) ON DELETE CASCADE, label TEXT)",
            "CREATE TABLE s74an_c(id INTEGER PRIMARY KEY, b_id INTEGER REFERENCES s74an_b(id) ON DELETE SET NULL, info TEXT)",
            "INSERT INTO s74an_a VALUES(1,'X'),(2,'Y')",
            "INSERT INTO s74an_b VALUES(10,1,'b1'),(20,1,'b2'),(30,2,'b3')",
            "INSERT INTO s74an_c VALUES(100,10,'c1'),(200,20,'c2'),(300,30,'c3')",
            "DELETE FROM s74an_a WHERE id = 1",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74an_a ORDER BY id",
            "SELECT * FROM s74an_b ORDER BY id",
            "SELECT * FROM s74an_c ORDER BY id",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74an mismatches", mismatches.len());
        }
    });
}

// ── s74ao: Correlated subquery in SELECT list ──

#[test]
fn test_conformance_correlated_subquery_select_list_s74ao() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ao_orders(id INTEGER PRIMARY KEY, cust_id INTEGER, total REAL)",
            "CREATE TABLE s74ao_cust(id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO s74ao_cust VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')",
            "INSERT INTO s74ao_orders VALUES(1,1,100.0),(2,1,200.0),(3,2,50.0),(4,2,75.0),(5,2,25.0)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT c.name, (SELECT COUNT(*) FROM s74ao_orders o WHERE o.cust_id = c.id) AS order_count FROM s74ao_cust c ORDER BY c.name",
            "SELECT c.name, (SELECT SUM(o.total) FROM s74ao_orders o WHERE o.cust_id = c.id) AS total_spent FROM s74ao_cust c ORDER BY c.name",
            "SELECT c.name, (SELECT MAX(o.total) FROM s74ao_orders o WHERE o.cust_id = c.id) AS max_order FROM s74ao_cust c ORDER BY c.name",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ao mismatches", mismatches.len());
        }
    });
}

// ── s74ap: Compound SELECT (UNION, INTERSECT, EXCEPT) edge cases ──

#[test]
fn test_conformance_compound_select_edges_s74ap() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ap_a(id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE TABLE s74ap_b(id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO s74ap_a VALUES(1,10),(2,20),(3,30),(4,40)",
            "INSERT INTO s74ap_b VALUES(1,20),(2,30),(3,50),(4,60)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT val FROM s74ap_a UNION SELECT val FROM s74ap_b ORDER BY val",
            "SELECT val FROM s74ap_a UNION ALL SELECT val FROM s74ap_b ORDER BY val",
            "SELECT val FROM s74ap_a INTERSECT SELECT val FROM s74ap_b ORDER BY val",
            "SELECT val FROM s74ap_a EXCEPT SELECT val FROM s74ap_b ORDER BY val",
            "SELECT val FROM s74ap_a UNION SELECT val FROM s74ap_b LIMIT 3 OFFSET 2",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ap mismatches", mismatches.len());
        }
    });
}

// ── s74aq: TRIGGER AFTER INSERT + UPDATE audit log ──

#[test]
fn test_conformance_trigger_after_insert_update_s74aq() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74aq_t(id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE TABLE s74aq_log(event TEXT, old_val INTEGER, new_val INTEGER)",
            "CREATE TRIGGER s74aq_ins AFTER INSERT ON s74aq_t BEGIN INSERT INTO s74aq_log VALUES('INSERT', NULL, NEW.val); END",
            "CREATE TRIGGER s74aq_upd AFTER UPDATE ON s74aq_t BEGIN INSERT INTO s74aq_log VALUES('UPDATE', OLD.val, NEW.val); END",
            "INSERT INTO s74aq_t VALUES(1,10),(2,20)",
            "UPDATE s74aq_t SET val = 15 WHERE id = 1",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74aq_t ORDER BY id",
            "SELECT * FROM s74aq_log ORDER BY rowid",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74aq mismatches", mismatches.len());
        }
    });
}

// ── s74ar: INSERT ... RETURNING ──

#[test]
fn test_conformance_insert_returning_s74ar() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let s = "CREATE TABLE s74ar_t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER DEFAULT 0)";
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
        let queries = &[
            "INSERT INTO s74ar_t(name, score) VALUES('Alice', 95) RETURNING id, name, score",
            "INSERT INTO s74ar_t(name) VALUES('Bob') RETURNING *",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ar mismatches", mismatches.len());
        }
    });
}

// ── s74as: Mixed-type sorting and comparison ──

#[test]
fn test_conformance_mixed_type_sort_s74as() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74as_t(id INTEGER PRIMARY KEY, v)",
            "INSERT INTO s74as_t VALUES(1, NULL),(2, 42),(3, 3.14),(4, 'hello'),(5, X'CAFE'),(6, 0),(7, ''),(8, -1)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, typeof(v), v FROM s74as_t ORDER BY id",
            "SELECT id, v FROM s74as_t WHERE v IS NOT NULL ORDER BY v",
            "SELECT id, v FROM s74as_t WHERE typeof(v) = 'integer' ORDER BY v",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74as mismatches", mismatches.len());
        }
    });
}

// ── s74at: DELETE with correlated EXISTS ──

#[test]
fn test_conformance_delete_correlated_exists_s74at() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74at_items(id INTEGER PRIMARY KEY, name TEXT, cat TEXT)",
            "CREATE TABLE s74at_keep(cat TEXT PRIMARY KEY)",
            "INSERT INTO s74at_items VALUES(1,'pen','office'),(2,'hammer','tools'),(3,'paper','office'),(4,'saw','tools'),(5,'tape','craft')",
            "INSERT INTO s74at_keep VALUES('office'),('craft')",
            "DELETE FROM s74at_items WHERE NOT EXISTS (SELECT 1 FROM s74at_keep WHERE s74at_keep.cat = s74at_items.cat)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &["SELECT * FROM s74at_items ORDER BY id"];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74at mismatches", mismatches.len());
        }
    });
}

// ── s74au: UPDATE with CASE expression in SET ──

#[test]
fn test_conformance_update_case_set_s74au() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74au_t(id INTEGER PRIMARY KEY, score INTEGER, grade TEXT DEFAULT '')",
            "INSERT INTO s74au_t VALUES(1,95,''),(2,85,''),(3,72,''),(4,58,''),(5,45,'')",
            "UPDATE s74au_t SET grade = CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' ELSE 'F' END",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &["SELECT * FROM s74au_t ORDER BY id"];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74au mismatches", mismatches.len());
        }
    });
}

// ── s74av: Math functions ──

#[test]
fn test_conformance_math_functions_s74av() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT abs(-42), abs(0), abs(3.14)",
            "SELECT max(1,2,3), min(1,2,3)",
            "SELECT round(2.5), round(3.14159, 2), round(-2.5)",
            "SELECT 17 % 5, 17 / 5, CAST(17 AS REAL) / 5",
            "SELECT sign(-5), sign(0), sign(5)",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74av mismatches", mismatches.len());
        }
    });
}

// ── s74aw: Recursive CTE series generation ──

#[test]
fn test_conformance_recursive_cte_series_s74aw() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10) SELECT x, x*x AS sq, x*x*x AS cube FROM cnt",
            "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 100) SELECT a FROM fib",
            "WITH RECURSIVE pow2(n, v) AS (SELECT 0, 1 UNION ALL SELECT n+1, v*2 FROM pow2 WHERE n < 8) SELECT n, v FROM pow2",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74aw mismatches", mismatches.len());
        }
    });
}

// ── s74ax: DISTINCT + ORDER BY + LIMIT combinations ──

#[test]
fn test_conformance_distinct_order_limit_s74ax() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ax_t(id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)",
            "INSERT INTO s74ax_t VALUES(1,'A',10),(2,'B',20),(3,'A',30),(4,'C',10),(5,'B',20),(6,'A',10),(7,'C',40)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT DISTINCT cat FROM s74ax_t ORDER BY cat",
            "SELECT DISTINCT val FROM s74ax_t ORDER BY val LIMIT 3",
            "SELECT DISTINCT cat, val FROM s74ax_t ORDER BY cat, val",
            "SELECT cat, COUNT(DISTINCT val) AS uniq FROM s74ax_t GROUP BY cat ORDER BY cat",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ax mismatches", mismatches.len());
        }
    });
}

// ── s74ay: UPSERT (INSERT ... ON CONFLICT) ──

#[test]
fn test_conformance_upsert_on_conflict_s74ay() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74ay_t(id INTEGER PRIMARY KEY, name TEXT UNIQUE, count INTEGER DEFAULT 1)",
            "INSERT INTO s74ay_t VALUES(1,'Alice',1),(2,'Bob',1)",
            "INSERT INTO s74ay_t(name, count) VALUES('Alice', 1) ON CONFLICT(name) DO UPDATE SET count = count + 1",
            "INSERT INTO s74ay_t(name, count) VALUES('Carol', 1) ON CONFLICT(name) DO UPDATE SET count = count + 1",
            "INSERT INTO s74ay_t(name, count) VALUES('Bob', 1) ON CONFLICT(name) DO UPDATE SET count = count + excluded.count",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &["SELECT name, count FROM s74ay_t ORDER BY name"];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ay mismatches", mismatches.len());
        }
    });
}

// ── s74az: Self-join for ranking ──

#[test]
fn test_conformance_self_join_ranking_s74az() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74az_t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
            "INSERT INTO s74az_t VALUES(1,'Alice',90),(2,'Bob',85),(3,'Carol',95),(4,'Dave',80)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT a.name, a.score, COUNT(b.id) + 1 AS rank FROM s74az_t a LEFT JOIN s74az_t b ON b.score > a.score GROUP BY a.id ORDER BY rank",
            "SELECT name, score FROM s74az_t WHERE score > (SELECT AVG(score) FROM s74az_t) ORDER BY score DESC",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74az mismatches", mismatches.len());
        }
    });
}

// ── s74ba: Datetime functions ──

#[test]
fn test_conformance_datetime_functions_s74ba() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT date('2026-03-15')",
            "SELECT time('14:30:00')",
            "SELECT datetime('2026-03-15 14:30:00')",
            "SELECT strftime('%Y', '2026-03-15')",
            "SELECT strftime('%m', '2026-03-15')",
            "SELECT strftime('%d', '2026-03-15')",
            "SELECT julianday('2026-01-01')",
            "SELECT date('2026-03-15', '+10 days')",
            "SELECT date('2026-03-15', '-1 month')",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74ba mismatches", mismatches.len());
        }
    });
}

// ── s74bb: CAST type coercion matrix ──

#[test]
fn test_conformance_cast_coercion_s74bb() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT CAST(42 AS TEXT), typeof(CAST(42 AS TEXT))",
            "SELECT CAST('123' AS INTEGER), typeof(CAST('123' AS INTEGER))",
            "SELECT CAST(3.14 AS INTEGER), typeof(CAST(3.14 AS INTEGER))",
            "SELECT CAST(0 AS REAL), typeof(CAST(0 AS REAL))",
            "SELECT CAST(NULL AS INTEGER), typeof(CAST(NULL AS INTEGER))",
            "SELECT CAST('3.14' AS REAL), typeof(CAST('3.14' AS REAL))",
            "SELECT CAST('' AS INTEGER)",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bb mismatches", mismatches.len());
        }
    });
}

// ── s74bc: LIKE/GLOB patterns ──

#[test]
fn test_conformance_like_glob_patterns_s74bc() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bc_t(id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO s74bc_t VALUES(1,'Alice'),(2,'Bob'),(3,'ALICE'),(4,'alice'),(5,'Al%ice'),(6,'Al_ice')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74bc_t WHERE name LIKE 'Al%' ORDER BY id",
            "SELECT * FROM s74bc_t WHERE name LIKE 'Al_ce' ORDER BY id",
            "SELECT * FROM s74bc_t WHERE name LIKE '%ice' ORDER BY id",
            "SELECT * FROM s74bc_t WHERE name GLOB 'Al*' ORDER BY id",
            "SELECT * FROM s74bc_t WHERE name GLOB 'Al?ce' ORDER BY id",
            "SELECT * FROM s74bc_t WHERE name LIKE 'Al$%ice' ESCAPE '$' ORDER BY id",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bc mismatches", mismatches.len());
        }
    });
}

// ── s74bd: Multiple CTEs with cross-references ──

#[test]
fn test_conformance_multi_cte_cross_ref_s74bd() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bd_t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
            "INSERT INTO s74bd_t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "WITH grp_totals AS (SELECT grp, SUM(val) AS total FROM s74bd_t GROUP BY grp), overall AS (SELECT SUM(total) AS grand FROM grp_totals) SELECT g.grp, g.total, ROUND(100.0 * g.total / o.grand, 1) AS pct FROM grp_totals g, overall o ORDER BY g.grp",
            "WITH mins AS (SELECT grp, MIN(val) AS mn FROM s74bd_t GROUP BY grp), maxs AS (SELECT grp, MAX(val) AS mx FROM s74bd_t GROUP BY grp) SELECT mins.grp, mins.mn, maxs.mx, maxs.mx - mins.mn AS range FROM mins JOIN maxs ON mins.grp = maxs.grp ORDER BY mins.grp",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bd mismatches", mismatches.len());
        }
    });
}

// ── s74be: Boolean NULL logic (three-valued) ──

#[test]
fn test_conformance_boolean_null_logic_s74be() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT NULL AND 1, NULL AND 0, NULL OR 1, NULL OR 0",
            "SELECT NOT NULL",
            "SELECT NULL = NULL, NULL != NULL, NULL IS NULL, NULL IS NOT NULL",
            "SELECT 1 IN (1, NULL), 2 IN (1, NULL), NULL IN (1, 2)",
            "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END",
            "SELECT COALESCE(NULL, NULL, 42)",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74be mismatches", mismatches.len());
        }
    });
}

// ── s74bf: ALTER TABLE ADD COLUMN + subsequent queries ──

#[test]
fn test_conformance_alter_table_add_col_s74bf() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bf_t(id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO s74bf_t VALUES(1,'Alice'),(2,'Bob')",
            "ALTER TABLE s74bf_t ADD COLUMN age INTEGER DEFAULT 0",
            "ALTER TABLE s74bf_t ADD COLUMN active INTEGER DEFAULT 1",
            "INSERT INTO s74bf_t VALUES(3,'Carol',30,1)",
            "UPDATE s74bf_t SET age = 25 WHERE id = 1",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74bf_t ORDER BY id",
            "SELECT name, age FROM s74bf_t WHERE active = 1 ORDER BY age DESC",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bf mismatches", mismatches.len());
        }
    });
}

// ── s74bg: GROUP_CONCAT with ORDER BY and separator variants ──

#[test]
fn test_conformance_group_concat_variants_s74bg() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bg_t(id INTEGER PRIMARY KEY, grp TEXT, val TEXT)",
            "INSERT INTO s74bg_t VALUES(1,'X','a'),(2,'X','b'),(3,'X','c'),(4,'Y','d'),(5,'Y','e'),(6,'Z','f')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT grp, GROUP_CONCAT(val) FROM s74bg_t GROUP BY grp ORDER BY grp",
            "SELECT grp, GROUP_CONCAT(val, '|') FROM s74bg_t GROUP BY grp ORDER BY grp",
            "SELECT GROUP_CONCAT(DISTINCT grp) FROM s74bg_t",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bg mismatches", mismatches.len());
        }
    });
}

// ── s74bh: Window LAG/LEAD ──

#[test]
fn test_conformance_window_lag_lead_s74bh() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bh_t(id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO s74bh_t VALUES(1,100),(2,200),(3,150),(4,300),(5,250)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT id, val, LAG(val, 1) OVER (ORDER BY id) AS prev_val FROM s74bh_t ORDER BY id",
            "SELECT id, val, LEAD(val, 1) OVER (ORDER BY id) AS next_val FROM s74bh_t ORDER BY id",
            "SELECT id, val, LAG(val, 2, 0) OVER (ORDER BY id) AS prev2 FROM s74bh_t ORDER BY id",
            "SELECT id, val, val - LAG(val, 1, val) OVER (ORDER BY id) AS delta FROM s74bh_t ORDER BY id",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bh mismatches", mismatches.len());
        }
    });
}

// ── s74bi: Nested subqueries in WHERE ──

#[test]
fn test_conformance_nested_subquery_where_s74bi() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bi_t(id INTEGER PRIMARY KEY, val INTEGER, cat TEXT)",
            "INSERT INTO s74bi_t VALUES(1,10,'A'),(2,20,'A'),(3,30,'B'),(4,40,'B'),(5,50,'C')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74bi_t WHERE val > (SELECT AVG(val) FROM s74bi_t) ORDER BY id",
            "SELECT * FROM s74bi_t WHERE cat IN (SELECT cat FROM s74bi_t GROUP BY cat HAVING COUNT(*) > 1) ORDER BY id",
            "SELECT * FROM s74bi_t WHERE val = (SELECT MAX(val) FROM s74bi_t WHERE cat = s74bi_t.cat) ORDER BY id",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bi mismatches", mismatches.len());
        }
    });
}

// ── s74bj: CREATE TABLE AS SELECT ──

#[test]
fn test_conformance_ctas_s74bj() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bj_src(id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
            "INSERT INTO s74bj_src VALUES(1,'Alice',10),(2,'Bob',20),(3,'Carol',30)",
            "CREATE TABLE s74bj_dst AS SELECT name, val * 2 AS doubled FROM s74bj_src WHERE val > 10",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74bj_dst ORDER BY name",
            "SELECT COUNT(*) FROM s74bj_dst",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bj mismatches", mismatches.len());
        }
    });
}

// ── s74bk: REPLACE INTO semantics ──

#[test]
fn test_conformance_replace_into_s74bk() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bk_t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
            "INSERT INTO s74bk_t VALUES(1,'Alice',10),(2,'Bob',20),(3,'Carol',30)",
            "REPLACE INTO s74bk_t VALUES(2,'Bobby',200)",
            "REPLACE INTO s74bk_t VALUES(4,'Dave',40)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74bk_t ORDER BY id",
            "SELECT COUNT(*) FROM s74bk_t",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bk mismatches", mismatches.len());
        }
    });
}

// ── s74bl: Arithmetic overflow and edge cases ──

#[test]
fn test_conformance_arithmetic_edges_s74bl() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let queries = &[
            "SELECT 9223372036854775807 + 0",
            "SELECT -9223372036854775808 + 0",
            "SELECT 1 / 0",
            "SELECT 0 / 0",
            "SELECT 1.0 / 3.0",
            "SELECT 10 % 3, -10 % 3",
            "SELECT ~0, ~1, ~(-1)",
            "SELECT 5 | 3, 5 & 3, 5 << 1, 20 >> 2",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bl mismatches", mismatches.len());
        }
    });
}

// ── s74bm: Derived table (subquery in FROM) with alias ──

#[test]
fn test_conformance_derived_table_alias_s74bm() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bm_t(id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)",
            "INSERT INTO s74bm_t VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT d.cat, d.total FROM (SELECT cat, SUM(val) AS total FROM s74bm_t GROUP BY cat) d ORDER BY d.total DESC",
            "SELECT d.cat, d.cnt FROM (SELECT cat, COUNT(*) AS cnt FROM s74bm_t GROUP BY cat HAVING COUNT(*) > 1) d ORDER BY d.cat",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bm mismatches", mismatches.len());
        }
    });
}

// ── s74bn: BETWEEN with various types ──

#[test]
fn test_conformance_between_types_s74bn() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in &[
            "CREATE TABLE s74bn_t(id INTEGER PRIMARY KEY, ival INTEGER, rval REAL, tval TEXT)",
            "INSERT INTO s74bn_t VALUES(1,10,1.5,'apple'),(2,20,2.5,'banana'),(3,30,3.5,'cherry'),(4,40,4.5,'date'),(5,50,5.5,'elderberry')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let queries = &[
            "SELECT * FROM s74bn_t WHERE ival BETWEEN 20 AND 40 ORDER BY id",
            "SELECT * FROM s74bn_t WHERE rval BETWEEN 2.0 AND 4.0 ORDER BY id",
            "SELECT * FROM s74bn_t WHERE tval BETWEEN 'b' AND 'd' ORDER BY id",
            "SELECT * FROM s74bn_t WHERE ival NOT BETWEEN 20 AND 40 ORDER BY id",
        ];
        let mismatches = oracle_compare(&fconn, &rconn, queries).await;
        if !mismatches.is_empty() {
            for m in &mismatches {
                eprintln!("{m}\n");
            }
            panic!("{} s74bn mismatches", mismatches.len());
        }
    });
}
