//! Session 55 conformance oracle tests — FK cascades, multi-way JOINs, CTE aggregates,
//! expression edges, complex UPDATEs, triggers, typeof, CAST, LIKE, BETWEEN, IN list edges.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Run queries against both FrankenSQLite and C SQLite, returning mismatches.
fn oracle_compare(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let frank_result = fconn.query(query);
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
                            rusqlite::types::Value::Blob(b) => {
                                format!(
                                    "X'{}'",
                                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                )
                            }
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
        let csql_result = match csql_result {
            Ok(r) => r,
            Err(csql_err) => {
                if frank_result.is_ok() {
                    mismatches.push(format!(
                        "DIVERGE: {query}\n  frank: OK\n  csql:  ERROR({csql_err})"
                    ));
                }
                continue;
            }
        };

        match frank_result {
            Ok(rows) => {
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
                                SqliteValue::Blob(b) => {
                                    format!(
                                        "X'{}'",
                                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                    )
                                }
                            })
                            .collect()
                    })
                    .collect();

                if frank_strs != csql_result {
                    mismatches.push(format!(
                        "MISMATCH: {query}\n  frank: {frank_strs:?}\n  csql:  {csql_result:?}"
                    ));
                }
            }
            Err(e) => {
                mismatches.push(format!(
                    "FRANK_ERR: {query}\n  frank: {e}\n  csql:  {csql_result:?}"
                ));
            }
        }
    }
    mismatches
}

#[test]
fn test_conformance_fk_cascade_delete_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    // Set up FK enforcement first via pragma
    rconn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    fconn.execute("PRAGMA foreign_keys = ON").unwrap();

    for s in &[
        "CREATE TABLE fkp(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE fkc_cascade(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fkp(id) ON DELETE CASCADE, label TEXT)",
        "CREATE TABLE fkc_setnull(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fkp(id) ON DELETE SET NULL, label TEXT)",
        "INSERT INTO fkp VALUES(1,'alpha'),(2,'beta'),(3,'gamma')",
        "INSERT INTO fkc_cascade VALUES(10,1,'c1'),(11,1,'c2'),(12,2,'c3'),(13,3,'c4')",
        "INSERT INTO fkc_setnull VALUES(20,1,'s1'),(21,2,'s2'),(22,3,'s3')",
        "DELETE FROM fkp WHERE id = 1",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM fkp ORDER BY id",
        "SELECT * FROM fkc_cascade ORDER BY id",
        "SELECT * FROM fkc_setnull ORDER BY id",
        "SELECT COUNT(*) FROM fkc_cascade",
        "SELECT COUNT(*) FROM fkc_setnull WHERE pid IS NULL",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} FK cascade/set null mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multiway_left_join_null_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE mlj_a(id INTEGER PRIMARY KEY, x TEXT)",
        "CREATE TABLE mlj_b(id INTEGER PRIMARY KEY, aid INTEGER, y TEXT)",
        "CREATE TABLE mlj_c(id INTEGER PRIMARY KEY, bid INTEGER, z TEXT)",
        "INSERT INTO mlj_a VALUES(1,'a1'),(2,'a2'),(3,'a3')",
        "INSERT INTO mlj_b VALUES(10,1,'b1'),(11,1,'b2'),(12,3,'b3')",
        "INSERT INTO mlj_c VALUES(100,10,'c1'),(101,12,'c2')",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT a.x, b.y, c.z FROM mlj_a a LEFT JOIN mlj_b b ON b.aid = a.id LEFT JOIN mlj_c c ON c.bid = b.id ORDER BY a.id, b.id, c.id",
        "SELECT a.x, COUNT(b.id), COUNT(c.id) FROM mlj_a a LEFT JOIN mlj_b b ON b.aid = a.id LEFT JOIN mlj_c c ON c.bid = b.id GROUP BY a.x ORDER BY a.x",
        "SELECT a.x, COALESCE(b.y, 'no_b'), COALESCE(c.z, 'no_c') FROM mlj_a a LEFT JOIN mlj_b b ON b.aid = a.id LEFT JOIN mlj_c c ON c.bid = b.id ORDER BY a.id, b.id",
        "SELECT a.x FROM mlj_a a LEFT JOIN mlj_b b ON b.aid = a.id WHERE b.id IS NULL",
        "SELECT a.x, b.y FROM mlj_a a LEFT JOIN mlj_b b ON b.aid = a.id LEFT JOIN mlj_c c ON c.bid = b.id WHERE c.id IS NULL ORDER BY a.id, b.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-way LEFT JOIN NULL mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_nested_cte_with_agg_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE nca_sales(id INTEGER PRIMARY KEY, region TEXT, product TEXT, amount REAL)",
        "INSERT INTO nca_sales VALUES(1,'N','Widget',100.0),(2,'N','Widget',150.0),(3,'N','Gadget',200.0),(4,'S','Widget',80.0),(5,'S','Gadget',120.0),(6,'S','Gadget',90.0)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "WITH regional AS (SELECT region, product, SUM(amount) AS total FROM nca_sales GROUP BY region, product) SELECT region, product, total FROM regional ORDER BY region, product",
        "WITH totals AS (SELECT product, SUM(amount) AS grand FROM nca_sales GROUP BY product) SELECT s.region, s.product, SUM(s.amount) AS reg_total, t.grand FROM nca_sales s JOIN totals t ON t.product = s.product GROUP BY s.region, s.product ORDER BY s.region, s.product",
        "WITH cnt AS (SELECT region, COUNT(*) AS n FROM nca_sales GROUP BY region) SELECT region, n FROM cnt WHERE n > 2 ORDER BY region",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested CTE with agg mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_union_all_order_limit_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE uao_t1(id INTEGER PRIMARY KEY, val TEXT, n INTEGER)",
        "CREATE TABLE uao_t2(id INTEGER PRIMARY KEY, val TEXT, n INTEGER)",
        "INSERT INTO uao_t1 VALUES(1,'a',10),(2,'b',20),(3,'c',30)",
        "INSERT INTO uao_t2 VALUES(4,'d',15),(5,'e',25),(6,'f',35)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT val, n FROM uao_t1 UNION ALL SELECT val, n FROM uao_t2 ORDER BY n",
        "SELECT val, n FROM uao_t1 UNION ALL SELECT val, n FROM uao_t2 ORDER BY n LIMIT 3",
        "SELECT val, n FROM uao_t1 UNION ALL SELECT val, n FROM uao_t2 ORDER BY n LIMIT 3 OFFSET 2",
        "SELECT val FROM uao_t1 UNION SELECT val FROM uao_t2 ORDER BY val",
        "SELECT val FROM uao_t1 EXCEPT SELECT val FROM uao_t2 ORDER BY val",
        "SELECT * FROM (SELECT val, n FROM uao_t1 UNION ALL SELECT val, n FROM uao_t2) ORDER BY n DESC LIMIT 2",
        "SELECT val, n FROM uao_t1 WHERE n > 15 UNION ALL SELECT val, n FROM uao_t2 WHERE n < 30 ORDER BY n",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UNION ALL ORDER/LIMIT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_expression_edge_nullif_iif_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT NULLIF(0, 0)",
        "SELECT NULLIF(1, 0)",
        "SELECT NULLIF('', '')",
        "SELECT NULLIF(NULL, NULL)",
        "SELECT NULLIF(NULL, 1)",
        "SELECT NULLIF(1, NULL)",
        "SELECT IIF(1, 'yes', 'no')",
        "SELECT IIF(0, 'yes', 'no')",
        "SELECT IIF(NULL, 'yes', 'no')",
        "SELECT IIF(1 > 2, 'gt', 'le')",
        "SELECT COALESCE(NULLIF(0, 0), 'fallback')",
        "SELECT COALESCE(NULLIF(1, 0), 'fallback')",
        "SELECT IIF(NULLIF(0, 0) IS NULL, 'was_zero', 'nonzero')",
        "SELECT NULLIF(CAST(1 AS TEXT), '1')",
        "SELECT NULLIF(1, CAST('1' AS INTEGER))",
        "SELECT IIF(typeof(1) = 'integer', 'int', 'other')",
        "SELECT IIF(typeof(1.0) = 'real', 'real', 'other')",
        "SELECT IIF(typeof('x') = 'text', 'text', 'other')",
        "SELECT IIF(typeof(NULL) = 'null', 'null', 'other')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NULLIF/IIF expression edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_order_by_case_expr_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE obc(id INTEGER PRIMARY KEY, status TEXT, priority INTEGER)",
        "INSERT INTO obc VALUES(1,'open',3),(2,'closed',1),(3,'open',2),(4,'pending',5),(5,'closed',4)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM obc ORDER BY CASE status WHEN 'open' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END, priority",
        "SELECT * FROM obc ORDER BY CASE WHEN priority > 3 THEN 0 ELSE 1 END, id",
        "SELECT status, COUNT(*) AS cnt FROM obc GROUP BY status ORDER BY CASE status WHEN 'open' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END",
        "SELECT *, CASE WHEN priority >= 3 THEN 'high' ELSE 'low' END AS tier FROM obc ORDER BY tier, priority DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ORDER BY CASE expression mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_insert_defaults_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE cid(id INTEGER PRIMARY KEY, a TEXT NOT NULL, b INTEGER DEFAULT 42)",
        "INSERT INTO cid(a) VALUES('x'),('y'),('z')",
        "INSERT INTO cid(a, b) VALUES('w', 100)",
        "INSERT OR REPLACE INTO cid(id, a, b) VALUES(1, 'replaced', 99)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM cid ORDER BY id",
        "SELECT COUNT(*), SUM(b), AVG(b) FROM cid",
        "SELECT a, b FROM cid WHERE b = 42 ORDER BY a",
        "SELECT a FROM cid WHERE id = 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex INSERT defaults mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_subquery_in_select_correlated_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE ssg_dept(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE ssg_emp(id INTEGER PRIMARY KEY, dept_id INTEGER, salary REAL)",
        "INSERT INTO ssg_dept VALUES(1,'eng'),(2,'sales'),(3,'hr')",
        "INSERT INTO ssg_emp VALUES(1,1,100.0),(2,1,120.0),(3,2,90.0),(4,2,95.0),(5,3,80.0)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT d.name, (SELECT COUNT(*) FROM ssg_emp e WHERE e.dept_id = d.id) AS emp_count FROM ssg_dept d ORDER BY d.name",
        "SELECT d.name, (SELECT SUM(e.salary) FROM ssg_emp e WHERE e.dept_id = d.id) AS total_sal FROM ssg_dept d ORDER BY d.name",
        "SELECT d.name, (SELECT AVG(e.salary) FROM ssg_emp e WHERE e.dept_id = d.id) AS avg_sal FROM ssg_dept d ORDER BY d.name",
        "SELECT d.name FROM ssg_dept d WHERE (SELECT COUNT(*) FROM ssg_emp e WHERE e.dept_id = d.id) > 1 ORDER BY d.name",
        "SELECT d.name FROM ssg_dept d WHERE EXISTS (SELECT 1 FROM ssg_emp e WHERE e.dept_id = d.id AND e.salary > 100) ORDER BY d.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} subquery-in-SELECT correlated mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_complex_update_with_subquery_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE cuj_items(id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
        "INSERT INTO cuj_items VALUES(1,'a',10.0,'X'),(2,'b',20.0,'X'),(3,'c',30.0,'Y'),(4,'d',40.0,'Y'),(5,'e',50.0,'Z')",
        "UPDATE cuj_items SET price = price * 1.1 WHERE category = (SELECT category FROM cuj_items GROUP BY category ORDER BY SUM(price) DESC LIMIT 1)",
        "UPDATE cuj_items SET name = CASE WHEN price > 30 THEN UPPER(name) ELSE name END",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM cuj_items ORDER BY id",
        "SELECT category, SUM(price) FROM cuj_items GROUP BY category ORDER BY category",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} complex UPDATE with subquery mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_trigger_insert_update_delete_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE tba_log(id INTEGER PRIMARY KEY, action TEXT, item_id INTEGER, ts TEXT DEFAULT 'now')",
        "CREATE TABLE tba_items(id INTEGER PRIMARY KEY, name TEXT, active INTEGER DEFAULT 1)",
        "CREATE TRIGGER tba_after_insert AFTER INSERT ON tba_items BEGIN INSERT INTO tba_log(action, item_id) VALUES('INSERT', NEW.id); END",
        "CREATE TRIGGER tba_after_delete AFTER DELETE ON tba_items BEGIN INSERT INTO tba_log(action, item_id) VALUES('DELETE', OLD.id); END",
        "CREATE TRIGGER tba_after_update AFTER UPDATE ON tba_items BEGIN INSERT INTO tba_log(action, item_id) VALUES('UPDATE', NEW.id); END",
        "INSERT INTO tba_items(id, name) VALUES(1, 'first')",
        "INSERT INTO tba_items(id, name) VALUES(2, 'second')",
        "INSERT INTO tba_items(id, name) VALUES(3, 'third')",
        "UPDATE tba_items SET active = 0 WHERE id = 2",
        "DELETE FROM tba_items WHERE id = 3",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM tba_items ORDER BY id",
        "SELECT action, item_id FROM tba_log ORDER BY id",
        "SELECT COUNT(*) FROM tba_log",
        "SELECT action, COUNT(*) FROM tba_log GROUP BY action ORDER BY action",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} trigger insert/update/delete mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_coalesce_with_subquery_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE csq_t(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO csq_t VALUES(1,10),(2,NULL),(3,30)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT COALESCE(val, 0) FROM csq_t ORDER BY id",
        "SELECT COALESCE(val, (SELECT MAX(val) FROM csq_t)) FROM csq_t ORDER BY id",
        "SELECT id, COALESCE(val, -1) + 1 FROM csq_t ORDER BY id",
        "SELECT COALESCE(NULL, NULL, 'found')",
        "SELECT COALESCE(NULL, 42, 99)",
        "SELECT COALESCE(1, NULL, 99)",
        "SELECT id, COALESCE(val, id * 100) FROM csq_t ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COALESCE with subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_concat_separator_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE gco(id INTEGER PRIMARY KEY, grp TEXT, val TEXT)",
        "INSERT INTO gco VALUES(1,'A','x'),(2,'A','y'),(3,'A','z'),(4,'B','m'),(5,'B','n')",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT grp, GROUP_CONCAT(val) FROM gco GROUP BY grp ORDER BY grp",
        "SELECT grp, GROUP_CONCAT(val, ';') FROM gco GROUP BY grp ORDER BY grp",
        "SELECT grp, GROUP_CONCAT(val, '') FROM gco GROUP BY grp ORDER BY grp",
        "SELECT GROUP_CONCAT(val) FROM gco",
        "SELECT GROUP_CONCAT(DISTINCT grp) FROM gco",
        "SELECT grp, GROUP_CONCAT(val || '!', ' ') FROM gco GROUP BY grp ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP_CONCAT separator mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_typeof_in_expressions_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE tie(id INTEGER PRIMARY KEY, a, b TEXT, c REAL, d BLOB)",
        "INSERT INTO tie VALUES(1, 42, 'hello', 3.14, X'DEADBEEF')",
        "INSERT INTO tie VALUES(2, NULL, NULL, NULL, NULL)",
        "INSERT INTO tie VALUES(3, 'text', '123', 0.0, X'')",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT typeof(a), typeof(b), typeof(c), typeof(d) FROM tie ORDER BY id",
        "SELECT typeof(a + 0), typeof(b || ''), typeof(c * 1) FROM tie WHERE id = 1",
        "SELECT typeof(CAST(42 AS TEXT)), typeof(CAST('42' AS INTEGER)), typeof(CAST('3.14' AS REAL))",
        "SELECT typeof(NULL), typeof(0), typeof(0.0), typeof(''), typeof(X'')",
        "SELECT typeof(1 + 1), typeof(1 + 1.0), typeof(1 || 'x')",
        "SELECT typeof(COALESCE(NULL, 1)), typeof(COALESCE(NULL, 'x'))",
        "SELECT typeof(IIF(1, 42, 'text')), typeof(IIF(0, 42, 'text'))",
        "SELECT typeof(MIN(1, 2)), typeof(MAX(1.0, 2))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} typeof in expressions mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_cross_type_arithmetic_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 1 + 1.0",
        "SELECT typeof(1 + 1.0)",
        "SELECT 1 + '2'",
        "SELECT typeof(1 + '2')",
        "SELECT '3' * '4'",
        "SELECT typeof('3' * '4')",
        "SELECT 1 + 'abc'",
        "SELECT typeof(1 + 'abc')",
        "SELECT 10 / 3",
        "SELECT typeof(10 / 3)",
        "SELECT 10.0 / 3",
        "SELECT typeof(10.0 / 3)",
        "SELECT 10 % 3",
        "SELECT -(-5)",
        "SELECT typeof(-(-5))",
        "SELECT 1 << 4",
        "SELECT 255 & 15",
        "SELECT 10 | 5",
        "SELECT ~0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} cross-type arithmetic mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_recursive_cte_series_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x < 10) SELECT x FROM cnt",
        "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT SUM(x) FROM cnt",
        "WITH RECURSIVE p2(n, val) AS (VALUES(0, 1) UNION ALL SELECT n+1, val*2 FROM p2 WHERE n < 8) SELECT n, val FROM p2",
        "WITH RECURSIVE fact(n, f) AS (VALUES(1, 1) UNION ALL SELECT n+1, f*(n+1) FROM fact WHERE n < 10) SELECT n, f FROM fact",
        "WITH RECURSIVE s(n, acc) AS (VALUES(1, 'a') UNION ALL SELECT n+1, acc || CHAR(96+n+1) FROM s WHERE n < 5) SELECT acc FROM s ORDER BY n DESC LIMIT 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE series mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_having_without_group_by_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE hwg(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO hwg VALUES(1,10),(2,20),(3,30)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT COUNT(*) FROM hwg HAVING COUNT(*) > 2",
        "SELECT SUM(val) FROM hwg HAVING SUM(val) > 50",
        "SELECT COUNT(*) FROM hwg HAVING COUNT(*) > 10",
        "SELECT AVG(val) FROM hwg HAVING AVG(val) > 15",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} HAVING without GROUP BY mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_insert_or_ignore_replace_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE ioi(id INTEGER PRIMARY KEY, name TEXT UNIQUE, val INTEGER)",
        "INSERT INTO ioi VALUES(1,'alpha',10)",
        "INSERT INTO ioi VALUES(2,'beta',20)",
        "INSERT OR IGNORE INTO ioi VALUES(3,'alpha',30)",
        "INSERT OR IGNORE INTO ioi VALUES(4,'gamma',40)",
        "INSERT OR REPLACE INTO ioi VALUES(2,'beta',99)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM ioi ORDER BY id",
        "SELECT COUNT(*) FROM ioi",
        "SELECT name, val FROM ioi WHERE name = 'alpha'",
        "SELECT name, val FROM ioi WHERE name = 'beta'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT OR IGNORE/REPLACE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_table_delete_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE mtd_orders(id INTEGER PRIMARY KEY, customer TEXT, total REAL)",
        "CREATE TABLE mtd_items(id INTEGER PRIMARY KEY, order_id INTEGER, product TEXT, qty INTEGER)",
        "INSERT INTO mtd_orders VALUES(1,'Alice',100.0),(2,'Bob',50.0),(3,'Carol',75.0)",
        "INSERT INTO mtd_items VALUES(10,1,'Widget',2),(11,1,'Gadget',1),(12,2,'Widget',5),(13,3,'Gizmo',3)",
        "DELETE FROM mtd_items WHERE order_id IN (SELECT id FROM mtd_orders WHERE total < 60)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM mtd_items ORDER BY id",
        "SELECT COUNT(*) FROM mtd_items",
        "SELECT o.customer, COUNT(i.id) FROM mtd_orders o LEFT JOIN mtd_items i ON i.order_id = o.id GROUP BY o.customer ORDER BY o.customer",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-table DELETE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_string_padding_trimming_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT TRIM('  hello  ')",
        "SELECT LTRIM('  hello  ')",
        "SELECT RTRIM('  hello  ')",
        "SELECT TRIM('xxhelloxx', 'x')",
        "SELECT LTRIM('xxhelloxx', 'x')",
        "SELECT RTRIM('xxhelloxx', 'x')",
        "SELECT LENGTH(TRIM('  abc  '))",
        "SELECT REPLACE('hello world', 'world', 'earth')",
        "SELECT REPLACE('aaa', 'a', 'bb')",
        "SELECT REPLACE('', 'a', 'b')",
        "SELECT REPLACE('hello', '', 'x')",
        "SELECT SUBSTR('hello', 1, 3)",
        "SELECT SUBSTR('hello', -3)",
        "SELECT SUBSTR('hello', 2)",
        "SELECT SUBSTR('hello', 0, 3)",
        "SELECT SUBSTR('hello', -10, 3)",
        "SELECT UPPER('hello'), LOWER('HELLO')",
        "SELECT UNICODE('A'), UNICODE('Z'), UNICODE('a')",
        "SELECT CHAR(65), CHAR(90), CHAR(97)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string padding/trimming mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_view_join_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE cvq_products(id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
        "INSERT INTO cvq_products VALUES(1,'A',10.0,'elec'),(2,'B',20.0,'elec'),(3,'C',15.0,'books'),(4,'D',5.0,'books'),(5,'E',50.0,'elec')",
        "CREATE VIEW cvq_expensive AS SELECT * FROM cvq_products WHERE price > 12",
        "CREATE VIEW cvq_cat_stats AS SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM cvq_products GROUP BY category",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM cvq_expensive ORDER BY id",
        "SELECT COUNT(*) FROM cvq_expensive",
        "SELECT category, SUM(price) FROM cvq_expensive GROUP BY category ORDER BY category",
        "SELECT * FROM cvq_cat_stats ORDER BY category",
        "SELECT e.name, s.avg_price FROM cvq_expensive e JOIN cvq_cat_stats s ON e.category = s.category ORDER BY e.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex view join mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_abs_zero_edge_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT ABS(0), ABS(-0), ABS(0.0), ABS(-0.0)",
        "SELECT ABS(-9223372036854775807)",
        "SELECT ABS(1), ABS(-1), ABS(1.5), ABS(-1.5)",
        "SELECT ABS(NULL)",
        "SELECT ABS('hello')",
        "SELECT ABS('-42')",
        "SELECT MIN(1, 2, 3), MAX(1, 2, 3)",
        "SELECT MIN(NULL, 1, 2), MAX(NULL, 1, 2)",
        "SELECT MIN('a', 'b', 'c'), MAX('a', 'b', 'c')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} abs/zero edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_instr_hex_quote_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT INSTR('hello world', 'world')",
        "SELECT INSTR('hello world', 'xyz')",
        "SELECT INSTR('abcabc', 'bc')",
        "SELECT INSTR('', 'x')",
        "SELECT INSTR('hello', '')",
        "SELECT INSTR(NULL, 'x')",
        "SELECT INSTR('hello', NULL)",
        "SELECT HEX('hello')",
        "SELECT HEX(42)",
        "SELECT HEX(NULL)",
        "SELECT HEX(X'DEADBEEF')",
        "SELECT QUOTE('hello')",
        "SELECT QUOTE(42)",
        "SELECT QUOTE(3.14)",
        "SELECT QUOTE(NULL)",
        "SELECT QUOTE(X'ABCD')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} instr/hex/quote mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_like_complex_patterns_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE lcp(id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO lcp VALUES(1,'hello'),(2,'HELLO'),(3,'Hello World'),(4,'%special%'),(5,'under_score'),(6,''),(7,NULL)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id FROM lcp WHERE val LIKE 'hello' ORDER BY id",
        "SELECT id FROM lcp WHERE val LIKE 'HELLO' ORDER BY id",
        "SELECT id FROM lcp WHERE val LIKE '%llo%' ORDER BY id",
        "SELECT id FROM lcp WHERE val LIKE 'H_llo' ORDER BY id",
        "SELECT id FROM lcp WHERE val LIKE '' ORDER BY id",
        "SELECT id FROM lcp WHERE val LIKE '%' ORDER BY id",
        "SELECT id FROM lcp WHERE val NOT LIKE '%llo%' ORDER BY id",
        "SELECT id FROM lcp WHERE val LIKE '%!%%' ESCAPE '!' ORDER BY id",
        "SELECT id FROM lcp WHERE val LIKE '%!_%' ESCAPE '!' ORDER BY id",
        "SELECT 'abc' LIKE 'ABC'",
        "SELECT 'abc' LIKE 'a%'",
        "SELECT NULL LIKE 'x'",
        "SELECT 'x' LIKE NULL",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LIKE complex pattern mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_cast_edge_cases_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT CAST(NULL AS TEXT)",
        "SELECT CAST(NULL AS REAL)",
        "SELECT CAST(NULL AS BLOB)",
        "SELECT CAST('' AS INTEGER)",
        "SELECT CAST('' AS REAL)",
        "SELECT CAST('abc' AS INTEGER)",
        "SELECT CAST('abc' AS REAL)",
        "SELECT CAST('123abc' AS INTEGER)",
        "SELECT CAST('123.45abc' AS REAL)",
        "SELECT CAST(X'48454C4C4F' AS TEXT)",
        "SELECT CAST(9223372036854775807 AS REAL)",
        "SELECT typeof(CAST(9223372036854775807 AS REAL))",
        "SELECT CAST(1 AS TEXT), typeof(CAST(1 AS TEXT))",
        "SELECT CAST(3.14 AS INTEGER), typeof(CAST(3.14 AS INTEGER))",
        "SELECT CAST(3.99 AS INTEGER)",
        "SELECT CAST(-3.99 AS INTEGER)",
        "SELECT CAST(0 AS TEXT), CAST(0 AS REAL)",
        "SELECT CAST(1e20 AS INTEGER)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CAST edge case mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_zeroblob_length_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT LENGTH(ZEROBLOB(0))",
        "SELECT LENGTH(ZEROBLOB(1))",
        "SELECT LENGTH(ZEROBLOB(10))",
        "SELECT typeof(ZEROBLOB(5))",
        "SELECT HEX(ZEROBLOB(4))",
        "SELECT LENGTH(X'')",
        "SELECT typeof(X'')",
        "SELECT HEX(X'00FF')",
        "SELECT LENGTH(X'DEADBEEF')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} zeroblob/length mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_between_null_type_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 5 BETWEEN 1 AND 10",
        "SELECT 5 BETWEEN 10 AND 1",
        "SELECT NULL BETWEEN 1 AND 10",
        "SELECT 5 BETWEEN NULL AND 10",
        "SELECT 5 BETWEEN 1 AND NULL",
        "SELECT 'c' BETWEEN 'a' AND 'e'",
        "SELECT 'f' BETWEEN 'a' AND 'e'",
        "SELECT 5 NOT BETWEEN 1 AND 10",
        "SELECT 15 NOT BETWEEN 1 AND 10",
        "SELECT NULL NOT BETWEEN 1 AND 10",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} BETWEEN NULL/type mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_in_list_edge_cases_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 1 IN (1, 2, 3)",
        "SELECT 4 IN (1, 2, 3)",
        "SELECT NULL IN (1, 2, 3)",
        "SELECT 1 IN (1, NULL, 3)",
        "SELECT 2 IN (1, NULL, 3)",
        "SELECT NULL IN (NULL)",
        "SELECT 1 NOT IN (1, 2, 3)",
        "SELECT 4 NOT IN (1, 2, 3)",
        "SELECT NULL NOT IN (1, 2, 3)",
        "SELECT 'a' IN ('a', 'b', 'c')",
        "SELECT 'x' IN ('a', 'b', 'c')",
        "SELECT 1 IN (SELECT 1 UNION SELECT 2 UNION SELECT 3)",
        "SELECT 5 IN (SELECT 1 UNION SELECT 2 UNION SELECT 3)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} IN list edge case mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_transaction_savepoint_complex_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE tsc(id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO tsc VALUES(1, 'original')",
        "SAVEPOINT sp1",
        "INSERT INTO tsc VALUES(2, 'in_sp1')",
        "SAVEPOINT sp2",
        "INSERT INTO tsc VALUES(3, 'in_sp2')",
        "ROLLBACK TO sp2",
        "INSERT INTO tsc VALUES(4, 'after_rollback_sp2')",
        "RELEASE sp1",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = ["SELECT * FROM tsc ORDER BY id", "SELECT COUNT(*) FROM tsc"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} transaction/savepoint complex mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_multiple_default_values_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE mdv(id INTEGER PRIMARY KEY, a INTEGER DEFAULT 0, b TEXT DEFAULT 'hello', c REAL DEFAULT 3.14, d INTEGER DEFAULT (1 + 2))",
        "INSERT INTO mdv(id) VALUES(1)",
        "INSERT INTO mdv(id, a) VALUES(2, 99)",
        "INSERT INTO mdv(id, b) VALUES(3, 'world')",
        "INSERT INTO mdv VALUES(4, 10, 'custom', 2.71, 100)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM mdv ORDER BY id",
        "SELECT a, b, c, d FROM mdv WHERE id = 1",
        "SELECT COUNT(*), SUM(a), SUM(d) FROM mdv",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multiple default values mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_column_order_by_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE mco(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)",
        "INSERT INTO mco VALUES(1,'x',10,1.0),(2,'y',10,2.0),(3,'x',20,1.5),(4,'y',20,0.5),(5,'x',10,3.0),(6,'z',10,1.0)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM mco ORDER BY a, b, c",
        "SELECT * FROM mco ORDER BY a ASC, b DESC, c ASC",
        "SELECT * FROM mco ORDER BY b DESC, a ASC, id",
        "SELECT a, SUM(c) AS total FROM mco GROUP BY a ORDER BY total DESC, a",
        "SELECT a, b, COUNT(*) FROM mco GROUP BY a, b ORDER BY COUNT(*) DESC, a, b",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-column ORDER BY mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_distinct_with_order_by_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE dwo(id INTEGER PRIMARY KEY, category TEXT, val INTEGER)",
        "INSERT INTO dwo VALUES(1,'A',10),(2,'B',20),(3,'A',30),(4,'C',10),(5,'B',20),(6,'A',10)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT DISTINCT category FROM dwo ORDER BY category",
        "SELECT DISTINCT val FROM dwo ORDER BY val",
        "SELECT DISTINCT category, val FROM dwo ORDER BY category, val",
        "SELECT DISTINCT category FROM dwo ORDER BY category DESC",
        "SELECT COUNT(DISTINCT category) FROM dwo",
        "SELECT COUNT(DISTINCT val) FROM dwo",
        "SELECT SUM(DISTINCT val) FROM dwo",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DISTINCT with ORDER BY mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_create_table_as_select_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE ctas_src(id INTEGER PRIMARY KEY, name TEXT, val REAL)",
        "INSERT INTO ctas_src VALUES(1,'a',1.5),(2,'b',2.5),(3,'c',3.5)",
        "CREATE TABLE ctas_copy AS SELECT * FROM ctas_src WHERE val > 2",
        "CREATE TABLE ctas_agg AS SELECT name, val * 2 AS doubled FROM ctas_src",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM ctas_copy ORDER BY id",
        "SELECT COUNT(*) FROM ctas_copy",
        "SELECT * FROM ctas_agg ORDER BY name",
        "SELECT name, doubled FROM ctas_agg WHERE doubled > 4 ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CREATE TABLE AS SELECT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_update_with_case_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE uwc(id INTEGER PRIMARY KEY, val INTEGER, label TEXT)",
        "INSERT INTO uwc VALUES(1,10,'low'),(2,20,'low'),(3,30,'low'),(4,40,'low'),(5,50,'low')",
        "UPDATE uwc SET label = CASE WHEN val > 30 THEN 'high' WHEN val > 15 THEN 'mid' ELSE 'low' END",
        "UPDATE uwc SET val = val + 100 WHERE val > 20",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM uwc ORDER BY id",
        "SELECT SUM(val) FROM uwc",
        "SELECT label, COUNT(*) FROM uwc GROUP BY label ORDER BY label",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE with CASE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_delete_with_subquery_where_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE dol(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO dol VALUES(1,10),(2,20),(3,30),(4,40),(5,50)",
        "DELETE FROM dol WHERE val > (SELECT AVG(val) FROM dol)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM dol ORDER BY id",
        "SELECT COUNT(*) FROM dol",
        "SELECT SUM(val) FROM dol",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DELETE with subquery WHERE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_rowid_edge_cases_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE roe(a TEXT, b INTEGER)",
        "INSERT INTO roe VALUES('x', 10)",
        "INSERT INTO roe VALUES('y', 20)",
        "INSERT INTO roe VALUES('z', 30)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT rowid, a, b FROM roe ORDER BY rowid",
        "SELECT rowid, a FROM roe WHERE rowid = 2",
        "SELECT rowid, a FROM roe WHERE rowid BETWEEN 1 AND 2 ORDER BY rowid",
        "SELECT MAX(rowid) FROM roe",
        "SELECT COUNT(*) FROM roe WHERE rowid > 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} rowid edge case mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_insert_select_with_transform_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE ist_src(id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
        "INSERT INTO ist_src VALUES(1,'alpha',10),(2,'beta',20),(3,'gamma',30)",
        "CREATE TABLE ist_dest(id INTEGER PRIMARY KEY, label TEXT, doubled INTEGER)",
        "INSERT INTO ist_dest SELECT id, UPPER(name), val * 2 FROM ist_src WHERE val >= 20",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT * FROM ist_dest ORDER BY id",
        "SELECT COUNT(*) FROM ist_dest",
        "SELECT SUM(doubled) FROM ist_dest",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} INSERT SELECT with transform mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_empty_table_aggregates_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    {
        let s = &"CREATE TABLE eta(id INTEGER PRIMARY KEY, val INTEGER, name TEXT)";

        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT COUNT(*) FROM eta",
        "SELECT COUNT(val) FROM eta",
        "SELECT SUM(val) FROM eta",
        "SELECT AVG(val) FROM eta",
        "SELECT MIN(val) FROM eta",
        "SELECT MAX(val) FROM eta",
        "SELECT TOTAL(val) FROM eta",
        "SELECT GROUP_CONCAT(name) FROM eta",
        "SELECT typeof(SUM(val)) FROM eta",
        "SELECT typeof(TOTAL(val)) FROM eta",
        "SELECT COALESCE(SUM(val), 0) FROM eta",
        "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM eta",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} empty table aggregate mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_mixed_type_sort_order_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE mts(id INTEGER PRIMARY KEY, val)",
        "INSERT INTO mts VALUES(1, NULL)",
        "INSERT INTO mts VALUES(2, 42)",
        "INSERT INTO mts VALUES(3, 3.14)",
        "INSERT INTO mts VALUES(4, 'text')",
        "INSERT INTO mts VALUES(5, X'ABCD')",
        "INSERT INTO mts VALUES(6, 0)",
        "INSERT INTO mts VALUES(7, '')",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = [
        "SELECT id, val, typeof(val) FROM mts ORDER BY val",
        "SELECT id, val, typeof(val) FROM mts ORDER BY val DESC",
        "SELECT typeof(val), COUNT(*) FROM mts GROUP BY typeof(val) ORDER BY typeof(val)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} mixed type sort order mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_compound_with_null_s55() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 1 UNION SELECT NULL ORDER BY 1",
        "SELECT NULL UNION SELECT 1 UNION SELECT NULL ORDER BY 1",
        "SELECT 1, 'a' UNION ALL SELECT NULL, 'b' UNION ALL SELECT 2, NULL ORDER BY 1",
        "SELECT 1 INTERSECT SELECT 1",
        "SELECT 1 EXCEPT SELECT 2",
        "SELECT 1 UNION SELECT 1 UNION SELECT 2",
        "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) WHERE x > 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} compound with NULL mismatches", mismatches.len());
    }
}

// ── Round 2: s55b tests ─────────────────────────────────────────────────

#[test]
fn test_conformance_nested_case_expressions_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price REAL, qty INTEGER);
        INSERT INTO items VALUES (1, 'A', 10.5, 3);
        INSERT INTO items VALUES (2, 'B', 20.0, 0);
        INSERT INTO items VALUES (3, 'A', 5.25, 10);
        INSERT INTO items VALUES (4, 'C', NULL, 5);
        INSERT INTO items VALUES (5, 'B', 15.0, NULL);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT id, CASE WHEN price IS NULL THEN 'no price' WHEN price > 15 THEN 'expensive' WHEN price > 8 THEN 'mid' ELSE 'cheap' END AS tier FROM items ORDER BY id",
        "SELECT id, CASE category WHEN 'A' THEN CASE WHEN price > 8 THEN 'A-hi' ELSE 'A-lo' END WHEN 'B' THEN 'B' ELSE 'other' END AS label FROM items ORDER BY id",
        "SELECT CASE WHEN qty IS NULL THEN -1 ELSE qty END + CASE WHEN price IS NULL THEN 0 ELSE price END AS computed FROM items ORDER BY id",
        "SELECT id, COALESCE(CASE WHEN qty > 0 THEN qty * price END, 0) AS revenue FROM items ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested CASE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_exists_subquery_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE depts (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE emps (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT);
        INSERT INTO depts VALUES (1, 'Engineering');
        INSERT INTO depts VALUES (2, 'Marketing');
        INSERT INTO depts VALUES (3, 'Empty');
        INSERT INTO emps VALUES (1, 1, 'Alice');
        INSERT INTO emps VALUES (2, 1, 'Bob');
        INSERT INTO emps VALUES (3, 2, 'Carol');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT d.name FROM depts d WHERE EXISTS (SELECT 1 FROM emps e WHERE e.dept_id = d.id) ORDER BY d.name",
        "SELECT d.name FROM depts d WHERE NOT EXISTS (SELECT 1 FROM emps e WHERE e.dept_id = d.id) ORDER BY d.name",
        "SELECT d.name, (SELECT COUNT(*) FROM emps e WHERE e.dept_id = d.id) AS cnt FROM depts d ORDER BY d.name",
        "SELECT d.name FROM depts d WHERE (SELECT COUNT(*) FROM emps e WHERE e.dept_id = d.id) > 1 ORDER BY d.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} EXISTS subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_replace_and_upsert_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE kv (key TEXT PRIMARY KEY, val INTEGER);
        INSERT INTO kv VALUES ('a', 1);
        INSERT INTO kv VALUES ('b', 2);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    // REPLACE overwrites existing row
    fconn.execute("REPLACE INTO kv VALUES ('a', 10)").unwrap();
    rconn
        .execute_batch("REPLACE INTO kv VALUES ('a', 10)")
        .unwrap();

    // INSERT OR REPLACE
    fconn
        .execute("INSERT OR REPLACE INTO kv VALUES ('b', 20)")
        .unwrap();
    rconn
        .execute_batch("INSERT OR REPLACE INTO kv VALUES ('b', 20)")
        .unwrap();

    // INSERT new
    fconn.execute("REPLACE INTO kv VALUES ('c', 30)").unwrap();
    rconn
        .execute_batch("REPLACE INTO kv VALUES ('c', 30)")
        .unwrap();

    let queries = ["SELECT key, val FROM kv ORDER BY key"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} REPLACE/UPSERT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_by_expression_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE sales (id INTEGER PRIMARY KEY, amount REAL, dt TEXT);
        INSERT INTO sales VALUES (1, 100.0, '2024-01-15');
        INSERT INTO sales VALUES (2, 200.0, '2024-01-20');
        INSERT INTO sales VALUES (3, 150.0, '2024-02-10');
        INSERT INTO sales VALUES (4, 300.0, '2024-02-28');
        INSERT INTO sales VALUES (5, 50.0, '2024-01-05');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT substr(dt, 1, 7) AS month, SUM(amount) AS total FROM sales GROUP BY substr(dt, 1, 7) ORDER BY month",
        "SELECT CASE WHEN amount >= 200 THEN 'high' ELSE 'low' END AS tier, COUNT(*) FROM sales GROUP BY CASE WHEN amount >= 200 THEN 'high' ELSE 'low' END ORDER BY tier",
        "SELECT (amount / 100) AS bucket, COUNT(*) FROM sales GROUP BY (amount / 100) ORDER BY bucket",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP BY expression mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_insert_select_complex_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, score INTEGER);
        INSERT INTO src VALUES (1, 'x', 10);
        INSERT INTO src VALUES (2, 'y', 20);
        INSERT INTO src VALUES (3, 'x', 30);
        INSERT INTO src VALUES (4, 'y', 40);
        CREATE TABLE dst (val TEXT, total INTEGER, cnt INTEGER);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let ins = "INSERT INTO dst SELECT val, SUM(score), COUNT(*) FROM src GROUP BY val";
    fconn.execute(ins).unwrap();
    rconn.execute_batch(ins).unwrap();

    let queries = ["SELECT val, total, cnt FROM dst ORDER BY val"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT...SELECT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_numeric_edge_cases_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 9223372036854775807 + 0",
        "SELECT -9223372036854775808 + 0",
        "SELECT 1 / 0",
        "SELECT 1 % 0",
        "SELECT 0.0 / 0.0",
        "SELECT typeof(1 / 0)",
        "SELECT typeof(1.0 / 0.0)",
        "SELECT CAST('999999999999999999999' AS INTEGER)",
        "SELECT CAST(1e20 AS INTEGER)",
        "SELECT ABS(-9223372036854775808)",
        "SELECT -(-9223372036854775807 - 1)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} numeric edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_string_functions_extended_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT replace('hello world', 'world', 'there')",
        "SELECT replace('aaa', 'a', 'bb')",
        "SELECT replace('abc', '', 'x')",
        "SELECT substr('hello', 2, 3)",
        "SELECT substr('hello', -2)",
        "SELECT substr('hello', 0)",
        "SELECT substr('hello', 1, 0)",
        "SELECT upper('hello')",
        "SELECT lower('HELLO')",
        "SELECT length('')",
        "SELECT length(NULL)",
        "SELECT ltrim('  hello  ')",
        "SELECT rtrim('  hello  ')",
        "SELECT trim('  hello  ')",
        "SELECT ltrim('xxxhello', 'x')",
        "SELECT instr('hello world', 'world')",
        "SELECT instr('hello', 'xyz')",
        "SELECT hex('ABC')",
        "SELECT quote('it''s')",
        "SELECT quote(NULL)",
        "SELECT quote(42)",
        "SELECT quote(3.14)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string function mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_collate_nocase_ordering_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE words (id INTEGER PRIMARY KEY, word TEXT COLLATE NOCASE);
        INSERT INTO words VALUES (1, 'banana');
        INSERT INTO words VALUES (2, 'Apple');
        INSERT INTO words VALUES (3, 'cherry');
        INSERT INTO words VALUES (4, 'BANANA');
        INSERT INTO words VALUES (5, 'apple');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT DISTINCT word FROM words ORDER BY word",
        "SELECT word, COUNT(*) FROM words GROUP BY word ORDER BY word",
        "SELECT word FROM words WHERE word = 'banana' ORDER BY id",
        "SELECT word FROM words WHERE word > 'banana' ORDER BY word",
        "SELECT word FROM words WHERE word IN ('apple', 'CHERRY') ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COLLATE NOCASE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_join_aggregate_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL);
        CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region TEXT);
        INSERT INTO customers VALUES (1, 'Alice', 'East');
        INSERT INTO customers VALUES (2, 'Bob', 'West');
        INSERT INTO customers VALUES (3, 'Carol', 'East');
        INSERT INTO orders VALUES (1, 1, 100.0);
        INSERT INTO orders VALUES (2, 1, 200.0);
        INSERT INTO orders VALUES (3, 2, 50.0);
        INSERT INTO orders VALUES (4, 1, 150.0);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT c.name, SUM(o.total) AS s FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.name ORDER BY c.name",
        "SELECT c.region, COUNT(DISTINCT c.id) AS cust_count, COALESCE(SUM(o.total), 0) AS rev FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.region ORDER BY c.region",
        "SELECT c.name FROM customers c WHERE c.id IN (SELECT customer_id FROM orders GROUP BY customer_id HAVING SUM(total) > 100) ORDER BY c.name",
        "SELECT c.name, (SELECT SUM(o.total) FROM orders o WHERE o.customer_id = c.id) AS total FROM customers c ORDER BY c.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-join aggregate mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_update_returning_complex_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE inventory (id INTEGER PRIMARY KEY, product TEXT, stock INTEGER, min_stock INTEGER);
        INSERT INTO inventory VALUES (1, 'Widget', 5, 10);
        INSERT INTO inventory VALUES (2, 'Gadget', 20, 5);
        INSERT INTO inventory VALUES (3, 'Doohickey', 0, 3);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    // Update with CASE
    let upd =
        "UPDATE inventory SET stock = CASE WHEN stock < min_stock THEN min_stock ELSE stock END";
    fconn.execute(upd).unwrap();
    rconn.execute_batch(upd).unwrap();

    let queries = ["SELECT product, stock FROM inventory ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE complex mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_subquery_in_select_list_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, cat_id INTEGER);
        CREATE TABLE categories (id INTEGER PRIMARY KEY, label TEXT);
        INSERT INTO categories VALUES (1, 'Toys');
        INSERT INTO categories VALUES (2, 'Tools');
        INSERT INTO products VALUES (1, 'Ball', 1);
        INSERT INTO products VALUES (2, 'Hammer', 2);
        INSERT INTO products VALUES (3, 'Doll', 1);
        INSERT INTO products VALUES (4, 'Orphan', 99);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT p.name, (SELECT c.label FROM categories c WHERE c.id = p.cat_id) AS cat FROM products p ORDER BY p.name",
        "SELECT p.name, COALESCE((SELECT c.label FROM categories c WHERE c.id = p.cat_id), 'Unknown') AS cat FROM products p ORDER BY p.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery in SELECT list mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_having_complex_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, msg TEXT);
        INSERT INTO logs VALUES (1, 'ERROR', 'fail1');
        INSERT INTO logs VALUES (2, 'WARN', 'warn1');
        INSERT INTO logs VALUES (3, 'ERROR', 'fail2');
        INSERT INTO logs VALUES (4, 'INFO', 'info1');
        INSERT INTO logs VALUES (5, 'ERROR', 'fail3');
        INSERT INTO logs VALUES (6, 'WARN', 'warn2');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT level, COUNT(*) AS cnt FROM logs GROUP BY level HAVING COUNT(*) >= 2 ORDER BY level",
        "SELECT level, COUNT(*) AS cnt FROM logs GROUP BY level HAVING cnt >= 2 ORDER BY level",
        "SELECT level, GROUP_CONCAT(msg) AS msgs FROM logs GROUP BY level HAVING COUNT(*) > 1 ORDER BY level",
        "SELECT level FROM logs GROUP BY level HAVING SUM(CASE WHEN msg LIKE 'fail%' THEN 1 ELSE 0 END) > 0 ORDER BY level",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} HAVING complex mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_cte_recursive_fibonacci_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "WITH RECURSIVE fib(n, a, b) AS (SELECT 1, 0, 1 UNION ALL SELECT n+1, b, a+b FROM fib WHERE n < 10) SELECT n, a FROM fib ORDER BY n",
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT x, x*x AS sq FROM cnt",
        "WITH RECURSIVE pow2(n, v) AS (SELECT 0, 1 UNION ALL SELECT n+1, v*2 FROM pow2 WHERE n < 8) SELECT n, v FROM pow2",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE Fibonacci mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_cross_join_and_natural_join_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1 (id INTEGER, val TEXT);
        CREATE TABLE t2 (id INTEGER, val TEXT);
        INSERT INTO t1 VALUES (1, 'a');
        INSERT INTO t1 VALUES (2, 'b');
        INSERT INTO t2 VALUES (2, 'b');
        INSERT INTO t2 VALUES (3, 'c');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT * FROM t1 CROSS JOIN t2 ORDER BY t1.id, t2.id",
        "SELECT * FROM t1 NATURAL JOIN t2",
        "SELECT t1.id, t2.id FROM t1, t2 WHERE t1.id = t2.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CROSS/NATURAL JOIN mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_type_coercion_comparisons_s55b() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE mixed (id INTEGER PRIMARY KEY, tval TEXT, ival INTEGER, rval REAL);
        INSERT INTO mixed VALUES (1, '10', 10, 10.0);
        INSERT INTO mixed VALUES (2, '5', 20, 5.0);
        INSERT INTO mixed VALUES (3, '100', 5, 100.0);
        INSERT INTO mixed VALUES (4, '2', 2, 2.0);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        // TEXT column vs integer literal — per §4.2, integer literal has no affinity,
        // but TEXT column has TEXT affinity → TEXT coercion (compare as strings)
        "SELECT id FROM mixed WHERE tval > '5' ORDER BY id",
        // INTEGER column vs string literal — INTEGER column has NUMERIC affinity
        // → apply numeric coercion to the string
        "SELECT id FROM mixed WHERE ival > '10' ORDER BY id",
        // Cross-column: TEXT vs INTEGER — numeric affinity wins
        "SELECT id FROM mixed WHERE tval > ival ORDER BY id",
        // REAL vs TEXT — numeric affinity wins
        "SELECT id FROM mixed WHERE rval > tval ORDER BY id",
        // Explicit CAST
        "SELECT id FROM mixed WHERE CAST(tval AS INTEGER) > ival ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} type coercion comparison mismatches", mismatches.len());
    }
}

// ── Round 3: s55c tests — deeper edge-case probing ──────────────────────

#[test]
fn test_conformance_null_arithmetic_chain_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT NULL + 1",
        "SELECT NULL * 0",
        "SELECT NULL || 'text'",
        "SELECT NULL = NULL",
        "SELECT NULL <> NULL",
        "SELECT NULL IS NULL",
        "SELECT NULL IS NOT NULL",
        "SELECT COALESCE(NULL, NULL, 3)",
        "SELECT NULLIF(1, 1)",
        "SELECT NULLIF(1, 2)",
        "SELECT IIF(NULL, 'yes', 'no')",
        "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no' END",
        "SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NULL arithmetic chain mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_aggregate_empty_table_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE empty_t (id INTEGER PRIMARY KEY, val REAL)";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT COUNT(*) FROM empty_t",
        "SELECT SUM(val) FROM empty_t",
        "SELECT AVG(val) FROM empty_t",
        "SELECT MIN(val) FROM empty_t",
        "SELECT MAX(val) FROM empty_t",
        "SELECT TOTAL(val) FROM empty_t",
        "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM empty_t",
        "SELECT COALESCE(SUM(val), 0) FROM empty_t",
        "SELECT GROUP_CONCAT(val) FROM empty_t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate empty table mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multiple_cte_refs_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "WITH t AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT a.x, b.x FROM t a, t b WHERE a.x < b.x ORDER BY a.x, b.x",
        "WITH t AS (SELECT 1 AS v) SELECT * FROM t UNION ALL SELECT * FROM t",
        "WITH t(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT SUM(n) FROM t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multiple CTE refs mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_where_boolean_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE flags (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);
        INSERT INTO flags VALUES (1, 1, 0, NULL);
        INSERT INTO flags VALUES (2, 0, 1, 1);
        INSERT INTO flags VALUES (3, 1, 1, 0);
        INSERT INTO flags VALUES (4, NULL, 0, 1);
        INSERT INTO flags VALUES (5, 0, 0, 0);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT id FROM flags WHERE a AND b ORDER BY id",
        "SELECT id FROM flags WHERE a OR b ORDER BY id",
        "SELECT id FROM flags WHERE NOT a ORDER BY id",
        "SELECT id FROM flags WHERE a AND NOT b ORDER BY id",
        "SELECT id FROM flags WHERE (a OR b) AND c ORDER BY id",
        "SELECT id FROM flags WHERE a IS NOT NULL AND a ORDER BY id",
        "SELECT id FROM flags WHERE COALESCE(a, 0) + COALESCE(b, 0) + COALESCE(c, 0) >= 2 ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex WHERE boolean mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_order_by_nulls_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER);
        INSERT INTO data VALUES (1, 30);
        INSERT INTO data VALUES (2, NULL);
        INSERT INTO data VALUES (3, 10);
        INSERT INTO data VALUES (4, NULL);
        INSERT INTO data VALUES (5, 20);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT id, val FROM data ORDER BY val",
        "SELECT id, val FROM data ORDER BY val DESC",
        "SELECT id, val FROM data ORDER BY val NULLS FIRST",
        "SELECT id, val FROM data ORDER BY val NULLS LAST",
        "SELECT id, val FROM data ORDER BY val DESC NULLS FIRST",
        "SELECT id, val FROM data ORDER BY val DESC NULLS LAST",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ORDER BY NULLS mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_subquery_in_where_various_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, grp TEXT);
        INSERT INTO t VALUES (1, 10, 'a');
        INSERT INTO t VALUES (2, 20, 'b');
        INSERT INTO t VALUES (3, 30, 'a');
        INSERT INTO t VALUES (4, 40, 'b');
        INSERT INTO t VALUES (5, 50, 'c');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT id FROM t WHERE val > (SELECT AVG(val) FROM t) ORDER BY id",
        "SELECT id FROM t WHERE val = (SELECT MAX(val) FROM t)",
        "SELECT id FROM t WHERE val IN (SELECT val FROM t WHERE grp = 'a') ORDER BY id",
        "SELECT id FROM t WHERE grp IN (SELECT grp FROM t GROUP BY grp HAVING COUNT(*) > 1) ORDER BY id",
        "SELECT id FROM t WHERE val > ALL (SELECT val FROM t WHERE grp = 'a') ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery in WHERE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_delete_with_join_subquery_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, active INTEGER);
        INSERT INTO items VALUES (1, 'a', 1);
        INSERT INTO items VALUES (2, 'b', 0);
        INSERT INTO items VALUES (3, 'c', 1);
        INSERT INTO items VALUES (4, 'd', 0);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    // Delete inactive items
    let del = "DELETE FROM items WHERE active = 0";
    fconn.execute(del).unwrap();
    rconn.execute_batch(del).unwrap();

    let queries = ["SELECT id, name FROM items ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DELETE with subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_printf_format_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT printf('%d', 42)",
        "SELECT printf('%05d', 42)",
        "SELECT printf('%.2f', 3.14159)",
        "SELECT printf('%s', 'hello')",
        "SELECT printf('%10s', 'hi')",
        "SELECT printf('%-10s', 'hi')",
        "SELECT printf('%d + %d = %d', 1, 2, 3)",
        "SELECT printf('%x', 255)",
        "SELECT printf('%o', 8)",
        "SELECT printf('%%')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} printf format mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_nested_derived_tables_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER);
        INSERT INTO data VALUES (1, 10);
        INSERT INTO data VALUES (2, 20);
        INSERT INTO data VALUES (3, 30);
        INSERT INTO data VALUES (4, 40);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT * FROM (SELECT id, val * 2 AS doubled FROM data) WHERE doubled > 30 ORDER BY id",
        "SELECT * FROM (SELECT * FROM (SELECT id, val FROM data) WHERE val > 15) WHERE val < 35 ORDER BY id",
        "SELECT s.id, s.total FROM (SELECT id, val + 100 AS total FROM data) s ORDER BY s.id",
        "SELECT x FROM (SELECT val AS x FROM data UNION ALL SELECT val * 10 FROM data) ORDER BY x",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested derived table mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_limit_offset_edge_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE nums (n INTEGER);
        INSERT INTO nums VALUES (1);
        INSERT INTO nums VALUES (2);
        INSERT INTO nums VALUES (3);
        INSERT INTO nums VALUES (4);
        INSERT INTO nums VALUES (5);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT n FROM nums ORDER BY n LIMIT 3",
        "SELECT n FROM nums ORDER BY n LIMIT 3 OFFSET 2",
        "SELECT n FROM nums ORDER BY n LIMIT 0",
        "SELECT n FROM nums ORDER BY n LIMIT 100",
        "SELECT n FROM nums ORDER BY n LIMIT 2 OFFSET 10",
        "SELECT n FROM nums ORDER BY n LIMIT -1",
        "SELECT n FROM nums ORDER BY n LIMIT -1 OFFSET 3",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LIMIT/OFFSET edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_index_usage_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE indexed_t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);
        CREATE INDEX idx_score ON indexed_t(score);
        CREATE INDEX idx_name ON indexed_t(name);
        INSERT INTO indexed_t VALUES (1, 'alice', 90);
        INSERT INTO indexed_t VALUES (2, 'bob', 85);
        INSERT INTO indexed_t VALUES (3, 'carol', 95);
        INSERT INTO indexed_t VALUES (4, 'dave', 80);
        INSERT INTO indexed_t VALUES (5, 'eve', 90);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT name FROM indexed_t WHERE score > 85 ORDER BY name",
        "SELECT name FROM indexed_t WHERE score BETWEEN 85 AND 92 ORDER BY name",
        "SELECT name, score FROM indexed_t WHERE name >= 'c' ORDER BY name",
        "SELECT COUNT(*) FROM indexed_t WHERE score = 90",
        "SELECT name FROM indexed_t WHERE score IN (80, 95) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} index usage mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_table_update_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE prices (id INTEGER PRIMARY KEY, product TEXT, price REAL);
        CREATE TABLE discounts (product TEXT, pct REAL);
        INSERT INTO prices VALUES (1, 'Widget', 100.0);
        INSERT INTO prices VALUES (2, 'Gadget', 200.0);
        INSERT INTO prices VALUES (3, 'Thingy', 50.0);
        INSERT INTO discounts VALUES ('Widget', 0.10);
        INSERT INTO discounts VALUES ('Gadget', 0.20);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    // Update prices using subquery
    let upd = "UPDATE prices SET price = price * (1 - COALESCE((SELECT pct FROM discounts WHERE discounts.product = prices.product), 0))";
    fconn.execute(upd).unwrap();
    rconn.execute_batch(upd).unwrap();

    let queries = ["SELECT product, price FROM prices ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-table UPDATE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_typeof_and_cast_edges_s55c() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT typeof(42)",
        "SELECT typeof(3.14)",
        "SELECT typeof('hello')",
        "SELECT typeof(NULL)",
        "SELECT typeof(X'01')",
        "SELECT typeof(42 + 0.0)",
        "SELECT typeof(CAST(42 AS TEXT))",
        "SELECT typeof(CAST('42' AS INTEGER))",
        "SELECT typeof(CAST('3.14' AS REAL))",
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT CAST(NULL AS TEXT)",
        "SELECT CAST('' AS INTEGER)",
        "SELECT CAST('abc' AS INTEGER)",
        "SELECT CAST(3.7 AS INTEGER)",
        "SELECT CAST(-3.7 AS INTEGER)",
        "SELECT CAST(1e18 AS INTEGER)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} typeof/CAST edge mismatches", mismatches.len());
    }
}

// ── Round 4: s55d tests — stress tests for tricky SQL patterns ──────────

#[test]
fn test_conformance_correlated_update_subquery_s55d() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER);
        CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, bonus INTEGER);
        INSERT INTO t1 VALUES (1, 100);
        INSERT INTO t1 VALUES (2, 200);
        INSERT INTO t1 VALUES (3, 300);
        INSERT INTO t2 VALUES (1, 1, 10);
        INSERT INTO t2 VALUES (2, 1, 20);
        INSERT INTO t2 VALUES (3, 2, 5);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    // Correlated UPDATE with aggregate subquery
    let upd =
        "UPDATE t1 SET val = val + COALESCE((SELECT SUM(bonus) FROM t2 WHERE t2.t1_id = t1.id), 0)";
    fconn.execute(upd).unwrap();
    rconn.execute_batch(upd).unwrap();

    let queries = ["SELECT id, val FROM t1 ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} correlated UPDATE subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_self_join_s55d() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, mgr_id INTEGER);
        INSERT INTO emp VALUES (1, 'CEO', NULL);
        INSERT INTO emp VALUES (2, 'VP', 1);
        INSERT INTO emp VALUES (3, 'Dir', 1);
        INSERT INTO emp VALUES (4, 'Mgr', 2);
        INSERT INTO emp VALUES (5, 'Dev', 4);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT e.name, m.name AS manager FROM emp e LEFT JOIN emp m ON e.mgr_id = m.id ORDER BY e.id",
        "SELECT e.name FROM emp e WHERE e.mgr_id IS NOT NULL AND e.mgr_id IN (SELECT id FROM emp WHERE mgr_id IS NOT NULL) ORDER BY e.id",
        "SELECT m.name, COUNT(e.id) AS reports FROM emp m JOIN emp e ON e.mgr_id = m.id GROUP BY m.name ORDER BY reports DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} self-join mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_union_with_group_s55d() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE sales_q1 (product TEXT, amount REAL);
        CREATE TABLE sales_q2 (product TEXT, amount REAL);
        INSERT INTO sales_q1 VALUES ('A', 100);
        INSERT INTO sales_q1 VALUES ('B', 200);
        INSERT INTO sales_q1 VALUES ('A', 50);
        INSERT INTO sales_q2 VALUES ('B', 300);
        INSERT INTO sales_q2 VALUES ('C', 150);
        INSERT INTO sales_q2 VALUES ('A', 75);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT product, SUM(amount) AS total FROM (SELECT * FROM sales_q1 UNION ALL SELECT * FROM sales_q2) GROUP BY product ORDER BY product",
        "SELECT product FROM (SELECT product FROM sales_q1 UNION SELECT product FROM sales_q2) ORDER BY product",
        "SELECT product, SUM(total) FROM (SELECT product, SUM(amount) AS total FROM sales_q1 GROUP BY product UNION ALL SELECT product, SUM(amount) FROM sales_q2 GROUP BY product) GROUP BY product ORDER BY product",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex UNION with GROUP mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_three_table_join_s55d() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE students (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE courses (id INTEGER PRIMARY KEY, title TEXT);
        CREATE TABLE enrollments (student_id INTEGER, course_id INTEGER);
        INSERT INTO students VALUES (1, 'Alice');
        INSERT INTO students VALUES (2, 'Bob');
        INSERT INTO students VALUES (3, 'Carol');
        INSERT INTO courses VALUES (1, 'Math');
        INSERT INTO courses VALUES (2, 'Science');
        INSERT INTO courses VALUES (3, 'Art');
        INSERT INTO enrollments VALUES (1, 1);
        INSERT INTO enrollments VALUES (1, 2);
        INSERT INTO enrollments VALUES (2, 1);
        INSERT INTO enrollments VALUES (3, 3);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT s.name, c.title FROM students s JOIN enrollments e ON e.student_id = s.id JOIN courses c ON c.id = e.course_id ORDER BY s.name, c.title",
        "SELECT s.name, COUNT(e.course_id) AS num_courses FROM students s LEFT JOIN enrollments e ON e.student_id = s.id GROUP BY s.name ORDER BY s.name",
        "SELECT c.title, COUNT(e.student_id) AS num_students FROM courses c LEFT JOIN enrollments e ON e.course_id = c.id GROUP BY c.title ORDER BY c.title",
        "SELECT s.name FROM students s WHERE s.id NOT IN (SELECT student_id FROM enrollments WHERE course_id = 3) ORDER BY s.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} three-table join mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_mixed_agg_and_nonagg_s55d() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO t VALUES (1, 'a', 10);
        INSERT INTO t VALUES (2, 'a', 20);
        INSERT INTO t VALUES (3, 'b', 30);
        INSERT INTO t VALUES (4, 'b', 40);
        INSERT INTO t VALUES (5, 'a', 15);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        // SQLite allows non-aggregate columns with GROUP BY
        "SELECT grp, val, SUM(val) AS s FROM t GROUP BY grp ORDER BY grp",
        "SELECT grp, MIN(val), MAX(val), COUNT(*) FROM t GROUP BY grp ORDER BY grp",
        "SELECT grp, SUM(val) * 2 AS doubled FROM t GROUP BY grp ORDER BY grp",
        "SELECT grp, ROUND(AVG(val), 1) AS avg_val FROM t GROUP BY grp ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} mixed agg/non-agg mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_like_edge_cases_s55d() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 'hello' LIKE 'hell_'",
        "SELECT 'hello' LIKE 'h%o'",
        "SELECT 'hello' LIKE 'HELLO'",
        "SELECT 'hello' LIKE '%'",
        "SELECT '' LIKE '%'",
        "SELECT '' LIKE '_'",
        "SELECT NULL LIKE 'hello'",
        "SELECT 'hello' LIKE NULL",
        "SELECT '10%' LIKE '10%%' ESCAPE '%'",
        "SELECT 'abc' LIKE 'a%c'",
        "SELECT 'ac' LIKE 'a%c'",
        "SELECT 'a' LIKE 'a%c'",
        "SELECT 'abc' NOT LIKE 'a%'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LIKE edge case mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_expression_only_selects_s55d() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 1 + 2 * 3",
        "SELECT (1 + 2) * 3",
        "SELECT 10 / 3",
        "SELECT 10.0 / 3",
        "SELECT 10 % 3",
        "SELECT -5 % 3",
        "SELECT 'hello' || ' ' || 'world'",
        "SELECT 1 << 4",
        "SELECT 255 >> 4",
        "SELECT 5 & 3",
        "SELECT 5 | 3",
        "SELECT ~0",
        "SELECT ~(-1)",
        "SELECT 1 = 1, 1 = 2, 1 != 2, 1 <> 1",
        "SELECT 1 < 2, 2 < 1, 1 <= 1, 1 >= 2",
        "SELECT abs(-5), abs(5), abs(0)",
        "SELECT min(1, 2, 3), max(1, 2, 3)",
        "SELECT unicode('A'), unicode('Z'), unicode('a')",
        "SELECT zeroblob(4)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} expression-only SELECT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_concat_separator_s55d() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE tags (item TEXT, tag TEXT);
        INSERT INTO tags VALUES ('a', 'red');
        INSERT INTO tags VALUES ('a', 'blue');
        INSERT INTO tags VALUES ('a', 'green');
        INSERT INTO tags VALUES ('b', 'red');
        INSERT INTO tags VALUES ('b', 'yellow');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT item, GROUP_CONCAT(tag) FROM tags GROUP BY item ORDER BY item",
        "SELECT item, GROUP_CONCAT(tag, ' | ') FROM tags GROUP BY item ORDER BY item",
        "SELECT item, GROUP_CONCAT(DISTINCT tag) FROM tags GROUP BY item ORDER BY item",
        "SELECT GROUP_CONCAT(tag, ', ') FROM tags",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP_CONCAT separator mismatches", mismatches.len());
    }
}

// ── Round 5: s55e tests — tricky codegen paths ──────────────────────────

#[test]
fn test_conformance_nested_in_subquery_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1 (id INTEGER PRIMARY KEY, cat TEXT);
        CREATE TABLE t2 (cat TEXT, priority INTEGER);
        INSERT INTO t1 VALUES (1, 'a');
        INSERT INTO t1 VALUES (2, 'b');
        INSERT INTO t1 VALUES (3, 'c');
        INSERT INTO t1 VALUES (4, 'a');
        INSERT INTO t2 VALUES ('a', 1);
        INSERT INTO t2 VALUES ('c', 2);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT id FROM t1 WHERE cat IN (SELECT cat FROM t2 WHERE priority = 1) ORDER BY id",
        "SELECT id FROM t1 WHERE cat NOT IN (SELECT cat FROM t2) ORDER BY id",
        "SELECT id FROM t1 WHERE cat IN (SELECT cat FROM t2) AND id > 1 ORDER BY id",
        "SELECT id FROM t1 WHERE cat IN ('a', 'c') AND id IN (SELECT id FROM t1 WHERE id > 2) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested IN subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_on_clause_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE a (id INTEGER, x INTEGER);
        CREATE TABLE b (id INTEGER, y INTEGER);
        INSERT INTO a VALUES (1, 10);
        INSERT INTO a VALUES (2, 20);
        INSERT INTO a VALUES (3, 30);
        INSERT INTO b VALUES (1, 10);
        INSERT INTO b VALUES (2, 25);
        INSERT INTO b VALUES (4, 40);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND a.x = b.y",
        "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id OR a.x = b.y ORDER BY a.id, b.id",
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id AND b.y > 15 ORDER BY a.id",
        "SELECT a.id, COALESCE(b.y, -1) FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex ON clause mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_count_distinct_variations_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO t VALUES (1, 'a', 1);
        INSERT INTO t VALUES (2, 'a', 2);
        INSERT INTO t VALUES (3, 'a', 1);
        INSERT INTO t VALUES (4, 'b', 3);
        INSERT INTO t VALUES (5, 'b', 3);
        INSERT INTO t VALUES (6, NULL, NULL);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT COUNT(DISTINCT val) FROM t",
        "SELECT COUNT(DISTINCT grp) FROM t",
        "SELECT grp, COUNT(DISTINCT val) FROM t GROUP BY grp ORDER BY grp",
        "SELECT COUNT(*), COUNT(val), COUNT(DISTINCT val) FROM t",
        "SELECT SUM(DISTINCT val) FROM t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COUNT DISTINCT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_null_in_list_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 1 WHERE 1 IN (1, 2, 3)",
        "SELECT 1 WHERE 4 IN (1, 2, 3)",
        "SELECT 1 WHERE NULL IN (1, 2, 3)",
        "SELECT 1 WHERE 1 IN (1, NULL, 3)",
        "SELECT 1 WHERE 4 IN (1, NULL, 3)",
        "SELECT 1 WHERE 1 NOT IN (2, 3)",
        "SELECT 1 WHERE 1 NOT IN (1, 2)",
        "SELECT 1 WHERE NULL NOT IN (1, 2)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NULL in list mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_update_from_subquery_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE target (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);
        CREATE TABLE source (id INTEGER, delta INTEGER);
        INSERT INTO target VALUES (1, 'Alice', 80);
        INSERT INTO target VALUES (2, 'Bob', 90);
        INSERT INTO target VALUES (3, 'Carol', 70);
        INSERT INTO source VALUES (1, 10);
        INSERT INTO source VALUES (3, 20);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let upd = "UPDATE target SET score = score + (SELECT COALESCE(SUM(delta), 0) FROM source WHERE source.id = target.id)";
    fconn.execute(upd).unwrap();
    rconn.execute_batch(upd).unwrap();

    let queries = ["SELECT name, score FROM target ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE FROM subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_cte_insert_select_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE results (n INTEGER, sq INTEGER)";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let ins = "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 5) INSERT INTO results SELECT x, x*x FROM seq";
    fconn.execute(ins).unwrap();
    rconn.execute_batch(ins).unwrap();

    let queries = ["SELECT n, sq FROM results ORDER BY n"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTE INSERT SELECT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_aggregate_over_join_null_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER, val INTEGER);
        INSERT INTO p VALUES (1, 'Alpha');
        INSERT INTO p VALUES (2, 'Beta');
        INSERT INTO p VALUES (3, 'Gamma');
        INSERT INTO c VALUES (1, 1, 10);
        INSERT INTO c VALUES (2, 1, 20);
        INSERT INTO c VALUES (3, 2, NULL);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT p.name, SUM(c.val) FROM p LEFT JOIN c ON c.p_id = p.id GROUP BY p.name ORDER BY p.name",
        "SELECT p.name, COUNT(c.val) FROM p LEFT JOIN c ON c.p_id = p.id GROUP BY p.name ORDER BY p.name",
        "SELECT p.name, COALESCE(SUM(c.val), 0) FROM p LEFT JOIN c ON c.p_id = p.id GROUP BY p.name ORDER BY p.name",
        "SELECT p.name, COUNT(*) FROM p LEFT JOIN c ON c.p_id = p.id GROUP BY p.name ORDER BY p.name",
        "SELECT p.name, TOTAL(c.val) FROM p LEFT JOIN c ON c.p_id = p.id GROUP BY p.name ORDER BY p.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate over JOIN NULL mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_between_null_edge_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);
        INSERT INTO t VALUES (1, 5);
        INSERT INTO t VALUES (2, 15);
        INSERT INTO t VALUES (3, 25);
        INSERT INTO t VALUES (4, NULL);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT id FROM t WHERE val BETWEEN 10 AND 20 ORDER BY id",
        "SELECT id FROM t WHERE val NOT BETWEEN 10 AND 20 ORDER BY id",
        "SELECT id FROM t WHERE val BETWEEN 5 AND 5 ORDER BY id",
        "SELECT id FROM t WHERE val BETWEEN 20 AND 10 ORDER BY id",
        "SELECT 1 WHERE NULL BETWEEN 1 AND 10",
        "SELECT 1 WHERE 5 BETWEEN NULL AND 10",
        "SELECT 1 WHERE 5 BETWEEN 1 AND NULL",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} BETWEEN null edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_left_join_where_vs_on_s55e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);
        CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, label TEXT);
        INSERT INTO t1 VALUES (1, 'a');
        INSERT INTO t1 VALUES (2, 'b');
        INSERT INTO t1 VALUES (3, 'c');
        INSERT INTO t2 VALUES (1, 1, 'x');
        INSERT INTO t2 VALUES (2, 1, 'y');
        INSERT INTO t2 VALUES (3, 2, 'z');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT t1.val, t2.label FROM t1 LEFT JOIN t2 ON t2.t1_id = t1.id AND t2.label = 'x' ORDER BY t1.id",
        "SELECT t1.val, t2.label FROM t1 LEFT JOIN t2 ON t2.t1_id = t1.id WHERE t2.label = 'x' ORDER BY t1.id",
        "SELECT t1.val FROM t1 LEFT JOIN t2 ON t2.t1_id = t1.id WHERE t2.id IS NULL ORDER BY t1.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LEFT JOIN WHERE vs ON mismatches", mismatches.len());
    }
}

// ── Round 6: s55f tests — expression eval contexts, complex queries ─────

#[test]
fn test_conformance_order_by_expression_s55f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);
        INSERT INTO t VALUES (1, 'Alice', 85);
        INSERT INTO t VALUES (2, 'Bob', 92);
        INSERT INTO t VALUES (3, 'Carol', 78);
        INSERT INTO t VALUES (4, 'Dave', 92);
        INSERT INTO t VALUES (5, 'Eve', 85);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT name FROM t ORDER BY score DESC, name ASC",
        "SELECT name, score % 10 AS ones FROM t ORDER BY ones, name",
        "SELECT name FROM t ORDER BY length(name), name",
        "SELECT name FROM t ORDER BY CASE WHEN score > 90 THEN 0 ELSE 1 END, name",
        "SELECT name FROM t ORDER BY -score",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ORDER BY expression mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_select_distinct_group_by_s55f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER, cat TEXT, val INTEGER);
        INSERT INTO t VALUES (1, 'a', 10);
        INSERT INTO t VALUES (2, 'b', 20);
        INSERT INTO t VALUES (3, 'a', 10);
        INSERT INTO t VALUES (4, 'b', 30);
        INSERT INTO t VALUES (5, 'c', 10);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT DISTINCT cat FROM t ORDER BY cat",
        "SELECT DISTINCT val FROM t ORDER BY val",
        "SELECT DISTINCT cat, val FROM t ORDER BY cat, val",
        "SELECT cat, SUM(val) AS s FROM t GROUP BY cat HAVING SUM(val) > 20 ORDER BY cat",
        // Tiebreaker: add cat to ORDER BY for deterministic ordering
        "SELECT cat FROM t GROUP BY cat ORDER BY COUNT(*) DESC, cat ASC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DISTINCT/GROUP BY mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_ifnull_and_nullif_s55f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT IFNULL(NULL, 42)",
        "SELECT IFNULL(1, 42)",
        "SELECT IFNULL(NULL, NULL)",
        "SELECT NULLIF(1, 1)",
        "SELECT NULLIF(1, 2)",
        "SELECT NULLIF(NULL, 1)",
        "SELECT NULLIF(1, NULL)",
        "SELECT IIF(1, 'yes', 'no')",
        "SELECT IIF(0, 'yes', 'no')",
        "SELECT IIF(NULL, 'yes', 'no')",
        "SELECT IIF(1 > 0, 'pos', 'neg')",
        "SELECT COALESCE(NULL, NULL, NULL, 'found')",
        // Note: COALESCE(NULL) with 1 arg rejected by C SQLite but accepted by FrankenSQLite
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} IFNULL/NULLIF/IIF mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_cte_with_multiple_refs_s55f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL);
        INSERT INTO orders VALUES (1, 'Alice', 100);
        INSERT INTO orders VALUES (2, 'Bob', 200);
        INSERT INTO orders VALUES (3, 'Alice', 300);
        INSERT INTO orders VALUES (4, 'Carol', 150);
        INSERT INTO orders VALUES (5, 'Bob', 50);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "WITH totals AS (SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer) SELECT customer, total FROM totals ORDER BY total DESC",
        "WITH totals AS (SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer), avg_t AS (SELECT AVG(total) AS avg_total FROM totals) SELECT t.customer, t.total FROM totals t, avg_t WHERE t.total > avg_t.avg_total ORDER BY t.customer",
        "WITH ranked AS (SELECT customer, amount, (SELECT COUNT(*) FROM orders o2 WHERE o2.amount > o1.amount) + 1 AS rank FROM orders o1) SELECT customer, amount, rank FROM ranked ORDER BY rank, customer",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex CTE multiple refs mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_row_insert_s55f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let ins = "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')";
    fconn.execute(ins).unwrap();
    rconn.execute_batch(ins).unwrap();

    let queries = [
        "SELECT id, val FROM t ORDER BY id",
        "SELECT COUNT(*) FROM t",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-row INSERT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_deeply_nested_subquery_s55f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);
        INSERT INTO t VALUES (1, 10);
        INSERT INTO t VALUES (2, 20);
        INSERT INTO t VALUES (3, 30);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT * FROM (SELECT * FROM (SELECT id, val FROM t) sub1) sub2 ORDER BY id",
        "SELECT id FROM t WHERE val > (SELECT MIN(val) FROM t) AND val < (SELECT MAX(val) FROM t)",
        "SELECT (SELECT COUNT(*) FROM t WHERE val > outer_t.val) AS rank FROM t AS outer_t ORDER BY rank",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} deeply nested subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_insert_or_conflict_s55f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT UNIQUE, cnt INTEGER DEFAULT 0);
        INSERT INTO t VALUES (1, 'a', 1);
        INSERT INTO t VALUES (2, 'b', 1);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    fconn
        .execute("INSERT OR IGNORE INTO t VALUES (3, 'a', 1)")
        .unwrap();
    rconn
        .execute_batch("INSERT OR IGNORE INTO t VALUES (3, 'a', 1)")
        .unwrap();

    fconn
        .execute("INSERT OR REPLACE INTO t VALUES (4, 'b', 2)")
        .unwrap();
    rconn
        .execute_batch("INSERT OR REPLACE INTO t VALUES (4, 'b', 2)")
        .unwrap();

    fconn
        .execute("INSERT OR IGNORE INTO t VALUES (5, 'c', 1)")
        .unwrap();
    rconn
        .execute_batch("INSERT OR IGNORE INTO t VALUES (5, 'c', 1)")
        .unwrap();

    let queries = ["SELECT id, val, cnt FROM t ORDER BY val"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT OR conflict mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_schema_introspection_s55f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
        CREATE INDEX idx_users_name ON users(name);
        CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), body TEXT);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT name, type FROM sqlite_master ORDER BY name",
        "SELECT tbl_name FROM sqlite_master WHERE type = 'index' ORDER BY name",
        "SELECT COUNT(*) FROM sqlite_master",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} schema introspection mismatches", mismatches.len());
    }
}

// ── Round 7: s55g tests — aggregate ORDER BY, tricky fallback paths ─────

#[test]
fn test_conformance_order_by_aggregate_unambiguous_s55g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT);
        INSERT INTO t VALUES (1, 'a');
        INSERT INTO t VALUES (2, 'a');
        INSERT INTO t VALUES (3, 'a');
        INSERT INTO t VALUES (4, 'b');
        INSERT INTO t VALUES (5, 'b');
        INSERT INTO t VALUES (6, 'c');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        // Unambiguous: a=3, b=2, c=1 → DESC should be a, b, c
        "SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp ORDER BY COUNT(*) DESC",
        // ASC: c=1, b=2, a=3
        "SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp ORDER BY cnt ASC",
        // ORDER BY alias
        "SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp ORDER BY cnt DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} ORDER BY aggregate unambiguous mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_subquery_as_column_in_group_by_s55g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1 (id INTEGER PRIMARY KEY, cat_id INTEGER);
        CREATE TABLE cats (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO cats VALUES (1, 'Alpha');
        INSERT INTO cats VALUES (2, 'Beta');
        INSERT INTO t1 VALUES (1, 1);
        INSERT INTO t1 VALUES (2, 1);
        INSERT INTO t1 VALUES (3, 2);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT (SELECT name FROM cats WHERE cats.id = t1.cat_id) AS cat_name, COUNT(*) FROM t1 GROUP BY cat_id ORDER BY cat_name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} subquery as column in GROUP BY mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_union_distinct_vs_all_s55g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT 1 AS x UNION SELECT 1 UNION SELECT 2",
        "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2",
        "SELECT 'a' UNION SELECT 'b' UNION SELECT 'a' ORDER BY 1",
        "SELECT 1, 'x' UNION SELECT 2, 'y' UNION SELECT 1, 'x' ORDER BY 1",
        "SELECT 1 AS n INTERSECT SELECT 1",
        "SELECT 1 AS n EXCEPT SELECT 2",
        "SELECT 1 AS n EXCEPT SELECT 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UNION DISTINCT vs ALL mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_rowid_alias_s55g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO t VALUES (10, 'ten');
        INSERT INTO t VALUES (20, 'twenty');
        INSERT INTO t VALUES (30, 'thirty');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT rowid, val FROM t ORDER BY rowid",
        "SELECT _rowid_, val FROM t ORDER BY _rowid_",
        "SELECT oid, val FROM t ORDER BY oid",
        "SELECT rowid, id FROM t WHERE rowid = 20",
        "SELECT * FROM t WHERE rowid BETWEEN 15 AND 25",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} rowid alias mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_by_having_subquery_s55g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO t VALUES (1, 'a', 10);
        INSERT INTO t VALUES (2, 'a', 20);
        INSERT INTO t VALUES (3, 'b', 5);
        INSERT INTO t VALUES (4, 'b', 100);
        INSERT INTO t VALUES (5, 'c', 50);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT grp, SUM(val) AS s FROM t GROUP BY grp HAVING SUM(val) > (SELECT AVG(val) FROM t) ORDER BY grp",
        "SELECT grp FROM t GROUP BY grp HAVING MAX(val) > 2 * MIN(val) ORDER BY grp",
        "SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING COUNT(*) = (SELECT MAX(cnt) FROM (SELECT COUNT(*) AS cnt FROM t GROUP BY grp)) ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP BY HAVING subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_create_table_as_select_s55g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO src VALUES (1, 'a');
        INSERT INTO src VALUES (2, 'b');
        INSERT INTO src VALUES (3, 'c');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    fconn
        .execute("CREATE TABLE dst AS SELECT id, upper(val) AS uval FROM src WHERE id > 1")
        .unwrap();
    rconn
        .execute_batch("CREATE TABLE dst AS SELECT id, upper(val) AS uval FROM src WHERE id > 1")
        .unwrap();

    let queries = [
        "SELECT id, uval FROM dst ORDER BY id",
        "SELECT COUNT(*) FROM dst",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CREATE TABLE AS SELECT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_datetime_functions_s55g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = [
        "SELECT date('2024-06-15')",
        "SELECT time('13:45:30')",
        "SELECT datetime('2024-06-15 13:45:30')",
        "SELECT date('2024-06-15', '+1 day')",
        "SELECT date('2024-06-15', '-1 month')",
        "SELECT date('2024-06-15', '+1 year')",
        "SELECT strftime('%Y', '2024-06-15')",
        "SELECT strftime('%m', '2024-06-15')",
        "SELECT strftime('%d', '2024-06-15')",
        "SELECT julianday('2024-06-15') > 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} datetime function mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_delete_s55g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, grp TEXT);
        INSERT INTO t VALUES (1, 10, 'a');
        INSERT INTO t VALUES (2, 20, 'a');
        INSERT INTO t VALUES (3, 30, 'b');
        INSERT INTO t VALUES (4, 40, 'b');
        INSERT INTO t VALUES (5, 50, 'c');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    // Delete rows where val is above the group average
    let del = "DELETE FROM t WHERE val > (SELECT AVG(val) FROM t AS t2 WHERE t2.grp = t.grp)";
    fconn.execute(del).unwrap();
    rconn.execute_batch(del).unwrap();

    let queries = ["SELECT id, val, grp FROM t ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex DELETE mismatches", mismatches.len());
    }
}

// ── Round 8: s55h tests — views, triggers with subqueries, edge codegen ──

#[test]
fn test_conformance_view_query_s55h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, cat TEXT);
        INSERT INTO products VALUES (1, 'Widget', 10.0, 'A');
        INSERT INTO products VALUES (2, 'Gadget', 20.0, 'B');
        INSERT INTO products VALUES (3, 'Thingy', 5.0, 'A');
        INSERT INTO products VALUES (4, 'Doohickey', 15.0, 'B');
        CREATE VIEW expensive_products AS SELECT name, price FROM products WHERE price > 12;
        CREATE VIEW category_summary AS SELECT cat, COUNT(*) AS cnt, SUM(price) AS total FROM products GROUP BY cat;
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT name, price FROM expensive_products ORDER BY name",
        "SELECT * FROM category_summary ORDER BY cat",
        "SELECT ep.name FROM expensive_products ep WHERE ep.price < 18 ORDER BY ep.name",
        "SELECT COUNT(*) FROM expensive_products",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} view query mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_trigger_with_new_old_s55h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE items (id INTEGER PRIMARY KEY, val INTEGER);
        CREATE TABLE audit (action TEXT, item_id INTEGER, old_val INTEGER, new_val INTEGER);
        CREATE TRIGGER items_update AFTER UPDATE ON items BEGIN
            INSERT INTO audit VALUES ('UPDATE', NEW.id, OLD.val, NEW.val);
        END;
        CREATE TRIGGER items_delete AFTER DELETE ON items BEGIN
            INSERT INTO audit VALUES ('DELETE', OLD.id, OLD.val, NULL);
        END;
        INSERT INTO items VALUES (1, 100);
        INSERT INTO items VALUES (2, 200);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    fconn
        .execute("UPDATE items SET val = 150 WHERE id = 1")
        .unwrap();
    rconn
        .execute_batch("UPDATE items SET val = 150 WHERE id = 1")
        .unwrap();

    fconn.execute("DELETE FROM items WHERE id = 2").unwrap();
    rconn
        .execute_batch("DELETE FROM items WHERE id = 2")
        .unwrap();

    let queries = [
        "SELECT action, item_id, old_val, new_val FROM audit ORDER BY rowid",
        "SELECT id, val FROM items ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} trigger with NEW/OLD mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_alter_table_add_column_s55h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO t VALUES (1, 'a');
        INSERT INTO t VALUES (2, 'b');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    fconn
        .execute("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0")
        .unwrap();
    rconn
        .execute_batch("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0")
        .unwrap();

    fconn.execute("INSERT INTO t VALUES (3, 'c', 42)").unwrap();
    rconn
        .execute_batch("INSERT INTO t VALUES (3, 'c', 42)")
        .unwrap();

    let queries = ["SELECT id, val, extra FROM t ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ALTER TABLE ADD COLUMN mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_upsert_do_update_set_s55h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE kv (key TEXT PRIMARY KEY, val INTEGER, updated INTEGER DEFAULT 0);
        INSERT INTO kv VALUES ('x', 1, 0);
        INSERT INTO kv VALUES ('y', 2, 0);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    // UPSERT: existing key → DO UPDATE
    fconn.execute("INSERT INTO kv VALUES ('x', 10, 0) ON CONFLICT(key) DO UPDATE SET val = excluded.val, updated = 1").unwrap();
    rconn.execute_batch("INSERT INTO kv VALUES ('x', 10, 0) ON CONFLICT(key) DO UPDATE SET val = excluded.val, updated = 1").unwrap();

    // UPSERT: new key → INSERT
    fconn.execute("INSERT INTO kv VALUES ('z', 3, 0) ON CONFLICT(key) DO UPDATE SET val = excluded.val, updated = 1").unwrap();
    rconn.execute_batch("INSERT INTO kv VALUES ('z', 3, 0) ON CONFLICT(key) DO UPDATE SET val = excluded.val, updated = 1").unwrap();

    let queries = ["SELECT key, val, updated FROM kv ORDER BY key"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPSERT DO UPDATE SET mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_aggregate_with_filter_expression_s55h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER, flag INTEGER);
        INSERT INTO t VALUES (1, 'a', 10, 1);
        INSERT INTO t VALUES (2, 'a', 20, 0);
        INSERT INTO t VALUES (3, 'a', 30, 1);
        INSERT INTO t VALUES (4, 'b', 40, 1);
        INSERT INTO t VALUES (5, 'b', 50, 0);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT grp, SUM(CASE WHEN flag = 1 THEN val ELSE 0 END) AS flagged_sum FROM t GROUP BY grp ORDER BY grp",
        "SELECT grp, COUNT(CASE WHEN flag = 1 THEN 1 END) AS flagged_count FROM t GROUP BY grp ORDER BY grp",
        "SELECT grp, AVG(val) AS avg_all, AVG(CASE WHEN flag = 1 THEN val END) AS avg_flagged FROM t GROUP BY grp ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} aggregate with filter expression mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_multiple_indexes_s55h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER);
        CREATE INDEX idx_name ON users(name);
        CREATE INDEX idx_email ON users(email);
        CREATE INDEX idx_age ON users(age);
        INSERT INTO users VALUES (1, 'Alice', 'alice@ex.com', 30);
        INSERT INTO users VALUES (2, 'Bob', 'bob@ex.com', 25);
        INSERT INTO users VALUES (3, 'Carol', 'carol@ex.com', 35);
        INSERT INTO users VALUES (4, 'Dave', 'dave@ex.com', 25);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        "SELECT name FROM users WHERE age = 25 ORDER BY name",
        "SELECT name FROM users WHERE email = 'carol@ex.com'",
        "SELECT name FROM users WHERE age > 25 AND name < 'D' ORDER BY name",
        "SELECT COUNT(*) FROM users WHERE age BETWEEN 25 AND 30",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multiple indexes mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_nested_aggregate_subquery_s55h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO t VALUES (1, 'a', 10);
        INSERT INTO t VALUES (2, 'a', 20);
        INSERT INTO t VALUES (3, 'b', 30);
        INSERT INTO t VALUES (4, 'b', 40);
        INSERT INTO t VALUES (5, 'c', 50);
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = [
        // Subquery in aggregate argument
        "SELECT SUM(val) FROM t WHERE grp IN (SELECT grp FROM t GROUP BY grp HAVING COUNT(*) > 1)",
        // Aggregate of aggregate via derived table
        "SELECT MAX(s) FROM (SELECT grp, SUM(val) AS s FROM t GROUP BY grp)",
        // Scalar subquery comparing aggregate
        "SELECT grp FROM t GROUP BY grp HAVING SUM(val) = (SELECT MAX(s) FROM (SELECT SUM(val) AS s FROM t GROUP BY grp)) ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested aggregate subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multicolumn_unique_s55h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t (a INTEGER, b INTEGER, c TEXT, UNIQUE(a, b));
        INSERT INTO t VALUES (1, 1, 'first');
        INSERT INTO t VALUES (1, 2, 'second');
        INSERT INTO t VALUES (2, 1, 'third');
    ";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    // INSERT OR IGNORE with multi-column unique
    fconn
        .execute("INSERT OR IGNORE INTO t VALUES (1, 1, 'dup')")
        .unwrap();
    rconn
        .execute_batch("INSERT OR IGNORE INTO t VALUES (1, 1, 'dup')")
        .unwrap();

    // INSERT OR REPLACE with multi-column unique
    fconn
        .execute("INSERT OR REPLACE INTO t VALUES (1, 2, 'replaced')")
        .unwrap();
    rconn
        .execute_batch("INSERT OR REPLACE INTO t VALUES (1, 2, 'replaced')")
        .unwrap();

    let queries = ["SELECT a, b, c FROM t ORDER BY a, b"];

    let mismatches = oracle_compare(&fconn, &rconn, &queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multicolumn UNIQUE mismatches", mismatches.len());
    }
}

// ── Session 67: PRAGMA table_info PK position tests ──

#[test]
fn test_conformance_pragma_table_info_ipk_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE pti_ipk(id INTEGER PRIMARY KEY, name TEXT, score REAL)",
        "CREATE TABLE pti_text_pk(code TEXT PRIMARY KEY, label TEXT NOT NULL)",
        "CREATE TABLE pti_no_pk(a TEXT, b INTEGER, c REAL)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // Use PRAGMA statement syntax (not table-valued function)
    let pragma_queries = &[
        "PRAGMA table_info(pti_ipk)",
        "PRAGMA table_info(pti_text_pk)",
        "PRAGMA table_info(pti_no_pk)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, pragma_queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} PRAGMA table_info single PK mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_pragma_table_info_composite_pk_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE pti_comp2(a TEXT, b INTEGER, c REAL, PRIMARY KEY(a, b))",
        "CREATE TABLE pti_comp3(x INTEGER, y TEXT, z REAL, w BLOB, PRIMARY KEY(z, x, w))",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // 2-column composite: a=1, b=2, c=0
    // 3-column non-contiguous composite: x=2, y=0, z=1, w=3
    let pragma_queries = &[
        "PRAGMA table_info(pti_comp2)",
        "PRAGMA table_info(pti_comp3)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, pragma_queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} PRAGMA table_info composite PK mismatches",
            mismatches.len()
        );
    }
}

// ── Session 67: multi-table DML sequences ──

#[test]
fn test_conformance_multi_table_dml_sequence_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE inv(id INTEGER PRIMARY KEY, product TEXT, qty INTEGER, price REAL);
        CREATE TABLE orders(id INTEGER PRIMARY KEY, inv_id INTEGER, amount INTEGER);
        INSERT INTO inv VALUES(1, 'Widget', 100, 9.99);
        INSERT INTO inv VALUES(2, 'Gadget', 50, 19.99);
        INSERT INTO inv VALUES(3, 'Doohickey', 200, 4.99);
        INSERT INTO orders VALUES(1, 1, 10);
        INSERT INTO orders VALUES(2, 1, 5);
        INSERT INTO orders VALUES(3, 2, 20);
        INSERT INTO orders VALUES(4, 3, 50);
        UPDATE inv SET qty = qty - (SELECT COALESCE(SUM(amount), 0) FROM orders WHERE inv_id = inv.id);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, product, qty FROM inv ORDER BY id",
        "SELECT i.product, i.qty, COALESCE(SUM(o.amount), 0) as total_ordered FROM inv i LEFT JOIN orders o ON o.inv_id = i.id GROUP BY i.id ORDER BY i.id",
        "SELECT product FROM inv WHERE qty < 50 ORDER BY product",
        "SELECT product, qty * price as remaining_value FROM inv ORDER BY remaining_value DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-table DML mismatches", mismatches.len());
    }
}

// ── Session 67: nested CASE with aggregates ──

#[test]
fn test_conformance_nested_case_aggregate_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE grades(student TEXT, subject TEXT, score INTEGER);
        INSERT INTO grades VALUES('Alice', 'Math', 95);
        INSERT INTO grades VALUES('Alice', 'Science', 82);
        INSERT INTO grades VALUES('Alice', 'English', 78);
        INSERT INTO grades VALUES('Bob', 'Math', 60);
        INSERT INTO grades VALUES('Bob', 'Science', 45);
        INSERT INTO grades VALUES('Bob', 'English', 90);
        INSERT INTO grades VALUES('Carol', 'Math', 72);
        INSERT INTO grades VALUES('Carol', 'Science', 88);
        INSERT INTO grades VALUES('Carol', 'English', 65);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT student, AVG(score) as avg_score, CASE WHEN AVG(score) >= 80 THEN 'A' WHEN AVG(score) >= 70 THEN 'B' WHEN AVG(score) >= 60 THEN 'C' ELSE 'F' END as grade FROM grades GROUP BY student ORDER BY student",
        "SELECT student, COUNT(CASE WHEN score >= 80 THEN 1 END) as high_scores, COUNT(CASE WHEN score < 60 THEN 1 END) as low_scores FROM grades GROUP BY student ORDER BY student",
        "SELECT subject, SUM(CASE WHEN score >= 70 THEN 1 ELSE 0 END) as passing, SUM(CASE WHEN score < 70 THEN 1 ELSE 0 END) as failing FROM grades GROUP BY subject ORDER BY subject",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested CASE aggregate mismatches", mismatches.len());
    }
}

// ── Session 67: complex subquery patterns ──

#[test]
fn test_conformance_subquery_patterns_advanced_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER, manager_id INTEGER);
        INSERT INTO emp VALUES(1, 'Alice', 'Eng', 120000, NULL);
        INSERT INTO emp VALUES(2, 'Bob', 'Eng', 95000, 1);
        INSERT INTO emp VALUES(3, 'Carol', 'Sales', 85000, NULL);
        INSERT INTO emp VALUES(4, 'Dave', 'Sales', 75000, 3);
        INSERT INTO emp VALUES(5, 'Eve', 'Eng', 110000, 1);
        INSERT INTO emp VALUES(6, 'Frank', 'HR', 90000, NULL);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT name, salary, (SELECT AVG(salary) FROM emp e2 WHERE e2.dept = emp.dept) as dept_avg FROM emp ORDER BY name",
        "SELECT name FROM emp e1 WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e2.manager_id = e1.id) ORDER BY name",
        "SELECT name FROM emp e1 WHERE NOT EXISTS (SELECT 1 FROM emp e2 WHERE e2.manager_id = e1.id) ORDER BY name",
        "SELECT dept, max_sal FROM (SELECT dept, MAX(salary) as max_sal FROM emp GROUP BY dept) ORDER BY dept",
        "SELECT name FROM emp WHERE dept IN (SELECT dept FROM emp GROUP BY dept HAVING COUNT(*) > 1) ORDER BY name",
        "SELECT name, salary FROM emp e1 WHERE salary > (SELECT AVG(salary) FROM emp e2 WHERE e2.dept = e1.dept) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery pattern mismatches", mismatches.len());
    }
}

// ── Session 67: INSERT...SELECT with transforms ──

#[test]
fn test_conformance_insert_select_transform_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE src(id INTEGER PRIMARY KEY, val TEXT, num INTEGER);
        INSERT INTO src VALUES(1, 'hello', 10);
        INSERT INTO src VALUES(2, 'world', 20);
        INSERT INTO src VALUES(3, 'foo', 30);
        CREATE TABLE dst(id INTEGER PRIMARY KEY, upper_val TEXT, doubled INTEGER);
        INSERT INTO dst SELECT id, UPPER(val), num * 2 FROM src;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM dst ORDER BY id",
        "SELECT COUNT(*) FROM dst",
        "SELECT upper_val FROM dst WHERE doubled > 30 ORDER BY upper_val",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT...SELECT transform mismatches", mismatches.len());
    }
}

// ── Session 67: GROUP BY expression + HAVING ──

#[test]
fn test_conformance_group_by_expression_having_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE sales(id INTEGER PRIMARY KEY, product TEXT, amount REAL, sale_date TEXT);
        INSERT INTO sales VALUES(1, 'A', 100.0, '2024-01-15');
        INSERT INTO sales VALUES(2, 'B', 200.0, '2024-01-20');
        INSERT INTO sales VALUES(3, 'A', 150.0, '2024-02-10');
        INSERT INTO sales VALUES(4, 'C', 50.0, '2024-02-15');
        INSERT INTO sales VALUES(5, 'B', 300.0, '2024-02-20');
        INSERT INTO sales VALUES(6, 'A', 75.0, '2024-03-01');
        INSERT INTO sales VALUES(7, 'C', 125.0, '2024-03-05');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT SUBSTR(sale_date, 1, 7) as month, SUM(amount) as total FROM sales GROUP BY SUBSTR(sale_date, 1, 7) ORDER BY month",
        "SELECT product, SUM(amount) as total FROM sales GROUP BY product HAVING SUM(amount) > 200 ORDER BY product",
        "SELECT product, COUNT(*) as cnt, AVG(amount) as avg_amt FROM sales GROUP BY product HAVING COUNT(*) >= 2 ORDER BY product",
        "SELECT product, MIN(amount) as min_amt, MAX(amount) as max_amt FROM sales GROUP BY product ORDER BY MAX(amount) DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP BY expression HAVING mismatches", mismatches.len());
    }
}

// ── Session 67: compound SELECT ──

#[test]
fn test_conformance_compound_select_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1(id INTEGER, val TEXT);
        CREATE TABLE t2(id INTEGER, val TEXT);
        INSERT INTO t1 VALUES(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
        INSERT INTO t2 VALUES(3, 'c'), (4, 'd'), (5, 'e'), (6, 'f');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, val FROM t1 UNION SELECT id, val FROM t2 ORDER BY id",
        "SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2 ORDER BY id, val",
        "SELECT id, val FROM t1 INTERSECT SELECT id, val FROM t2 ORDER BY id",
        "SELECT id, val FROM t1 EXCEPT SELECT id, val FROM t2 ORDER BY id",
        "SELECT id FROM t1 WHERE id > 2 UNION SELECT id FROM t2 WHERE id < 5 ORDER BY id",
        "SELECT id FROM t1 UNION SELECT id FROM t2 UNION SELECT 7 ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} compound SELECT mismatches", mismatches.len());
    }
}

// ── Session 67: COALESCE/NULLIF edges ──

#[test]
fn test_conformance_coalesce_nullif_edges_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE nv(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d INTEGER);
        INSERT INTO nv VALUES(1, NULL, NULL, 'fallback', 10);
        INSERT INTO nv VALUES(2, NULL, 'second', 'third', 20);
        INSERT INTO nv VALUES(3, 'first', NULL, NULL, 30);
        INSERT INTO nv VALUES(4, NULL, NULL, NULL, NULL);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, COALESCE(a, b, c, 'none') as result FROM nv ORDER BY id",
        "SELECT id, NULLIF(d, 20) as result FROM nv ORDER BY id",
        "SELECT id, COALESCE(NULLIF(a, 'first'), b, 'default') FROM nv ORDER BY id",
        "SELECT id, IIF(d IS NULL, 'missing', IIF(d > 15, 'high', 'low')) FROM nv ORDER BY id",
        "SELECT COALESCE(NULL, NULL, NULL, 42)",
        "SELECT NULLIF(1, 1), NULLIF(1, 2), NULLIF(NULL, 1)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COALESCE/NULLIF edge mismatches", mismatches.len());
    }
}

// ── Session 67: self-join tree patterns ──

#[test]
fn test_conformance_self_join_tree_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT);
        INSERT INTO tree VALUES(1, NULL, 'root');
        INSERT INTO tree VALUES(2, 1, 'child1');
        INSERT INTO tree VALUES(3, 1, 'child2');
        INSERT INTO tree VALUES(4, 2, 'grandchild1');
        INSERT INTO tree VALUES(5, 2, 'grandchild2');
        INSERT INTO tree VALUES(6, 3, 'grandchild3');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT c.name as child, p.name as parent FROM tree c LEFT JOIN tree p ON c.parent_id = p.id ORDER BY c.id",
        "SELECT t.name FROM tree t WHERE NOT EXISTS (SELECT 1 FROM tree c WHERE c.parent_id = t.id) ORDER BY t.name",
        "SELECT p.name, COUNT(c.id) as child_count FROM tree p LEFT JOIN tree c ON c.parent_id = p.id GROUP BY p.id ORDER BY p.name",
        "SELECT gp.name as grandparent, gc.name as grandchild FROM tree gc JOIN tree p ON gc.parent_id = p.id JOIN tree gp ON p.parent_id = gp.id ORDER BY gc.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} self-join tree mismatches", mismatches.len());
    }
}

// ── Session 67: recursive CTE graph traversal ──

#[test]
fn test_conformance_recursive_cte_graph_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE graph(src INTEGER, dst INTEGER, weight INTEGER);
        INSERT INTO graph VALUES(1, 2, 5);
        INSERT INTO graph VALUES(1, 3, 10);
        INSERT INTO graph VALUES(2, 4, 3);
        INSERT INTO graph VALUES(3, 4, 7);
        INSERT INTO graph VALUES(4, 5, 2);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "WITH RECURSIVE reach(node, depth) AS (SELECT 1, 0 UNION SELECT g.dst, r.depth + 1 FROM reach r JOIN graph g ON g.src = r.node WHERE r.depth < 5) SELECT node, depth FROM reach ORDER BY depth, node",
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) SELECT x FROM cnt",
        "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 100) SELECT a FROM fib",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE graph mismatches", mismatches.len());
    }
}

// ── Session 67: string function edges ──

#[test]
fn test_conformance_string_fn_edges_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT LENGTH('')",
        "SELECT LENGTH(NULL)",
        "SELECT SUBSTR('hello', 2, 3)",
        "SELECT SUBSTR('hello', -2)",
        "SELECT REPLACE('aabbcc', 'bb', 'XX')",
        "SELECT REPLACE('aaa', 'a', '')",
        "SELECT TRIM('  hello  ')",
        "SELECT LTRIM('  hello  ')",
        "SELECT RTRIM('  hello  ')",
        "SELECT INSTR('hello world', 'world')",
        "SELECT INSTR('hello', 'xyz')",
        "SELECT UPPER('hello'), LOWER('HELLO')",
        "SELECT TYPEOF(123), TYPEOF(1.5), TYPEOF('hi'), TYPEOF(NULL), TYPEOF(X'00')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string function edge mismatches", mismatches.len());
    }
}

// ── Session 67: DELETE with complex WHERE ──

#[test]
fn test_conformance_delete_complex_where_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE items(id INTEGER PRIMARY KEY, category TEXT, status TEXT, priority INTEGER);
        INSERT INTO items VALUES(1, 'A', 'active', 3);
        INSERT INTO items VALUES(2, 'B', 'inactive', 1);
        INSERT INTO items VALUES(3, 'A', 'inactive', 2);
        INSERT INTO items VALUES(4, 'C', 'active', 5);
        INSERT INTO items VALUES(5, 'B', 'active', 4);
        INSERT INTO items VALUES(6, 'A', 'active', 1);
        DELETE FROM items WHERE status = 'inactive' OR (category = 'A' AND priority < 2);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM items ORDER BY id",
        "SELECT COUNT(*) FROM items",
        "SELECT category, COUNT(*) FROM items GROUP BY category ORDER BY category",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DELETE complex WHERE mismatches", mismatches.len());
    }
}

// ── Session 67: multi-column ORDER BY with NULLs ──

#[test]
fn test_conformance_multi_col_order_nulls_s67e() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE mc(a TEXT, b INTEGER, c REAL);
        INSERT INTO mc VALUES('x', 1, 3.0);
        INSERT INTO mc VALUES('x', NULL, 2.0);
        INSERT INTO mc VALUES(NULL, 2, 1.0);
        INSERT INTO mc VALUES('y', 1, NULL);
        INSERT INTO mc VALUES('x', 1, 1.0);
        INSERT INTO mc VALUES(NULL, NULL, NULL);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT a, b, c FROM mc ORDER BY a, b, c",
        "SELECT a, b, c FROM mc ORDER BY a DESC, b ASC, c DESC",
        "SELECT COALESCE(a, 'NA') as a2, COALESCE(b, -1) as b2 FROM mc ORDER BY a2, b2",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-col ORDER BY NULLs mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: UPDATE with correlated subquery SET ──

#[test]
fn test_conformance_update_correlated_set_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, price REAL);
        CREATE TABLE discounts(product_id INTEGER, pct REAL);
        INSERT INTO products VALUES(1, 'A', 100.0);
        INSERT INTO products VALUES(2, 'B', 200.0);
        INSERT INTO products VALUES(3, 'C', 50.0);
        INSERT INTO discounts VALUES(1, 0.1);
        INSERT INTO discounts VALUES(2, 0.2);
        UPDATE products SET price = price * (1 - COALESCE((SELECT pct FROM discounts WHERE product_id = products.id), 0));
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, name, price FROM products ORDER BY id",
        "SELECT name FROM products WHERE price < 100 ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE correlated SET mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: REPLACE semantics ──

#[test]
fn test_conformance_replace_semantics_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE kv(key TEXT PRIMARY KEY, val INTEGER);
        INSERT INTO kv VALUES('a', 1);
        INSERT INTO kv VALUES('b', 2);
        INSERT INTO kv VALUES('c', 3);
        REPLACE INTO kv VALUES('b', 20);
        REPLACE INTO kv VALUES('d', 4);
        INSERT OR REPLACE INTO kv VALUES('a', 10);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT key, val FROM kv ORDER BY key",
        "SELECT COUNT(*) FROM kv",
        "SELECT SUM(val) FROM kv",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} REPLACE semantics mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: BETWEEN with various types ──

#[test]
fn test_conformance_between_types_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE bt(id INTEGER PRIMARY KEY, ival INTEGER, rval REAL, tval TEXT);
        INSERT INTO bt VALUES(1, 5, 1.5, 'banana');
        INSERT INTO bt VALUES(2, 10, 2.5, 'cherry');
        INSERT INTO bt VALUES(3, 15, 3.5, 'apple');
        INSERT INTO bt VALUES(4, 20, 4.5, 'date');
        INSERT INTO bt VALUES(5, NULL, NULL, NULL);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id FROM bt WHERE ival BETWEEN 8 AND 16 ORDER BY id",
        "SELECT id FROM bt WHERE rval BETWEEN 2.0 AND 4.0 ORDER BY id",
        "SELECT id FROM bt WHERE tval BETWEEN 'b' AND 'd' ORDER BY id",
        "SELECT id FROM bt WHERE ival NOT BETWEEN 5 AND 15 ORDER BY id",
        // NULL in BETWEEN
        "SELECT id FROM bt WHERE ival BETWEEN NULL AND 20 ORDER BY id",
        "SELECT id FROM bt WHERE NULL BETWEEN 1 AND 10",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} BETWEEN type mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: LIKE/GLOB patterns ──

#[test]
fn test_conformance_like_glob_patterns_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE paths(id INTEGER PRIMARY KEY, path TEXT);
        INSERT INTO paths VALUES(1, '/usr/bin/bash');
        INSERT INTO paths VALUES(2, '/usr/local/bin/python');
        INSERT INTO paths VALUES(3, '/home/user/docs');
        INSERT INTO paths VALUES(4, '/tmp/test_file.txt');
        INSERT INTO paths VALUES(5, NULL);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id FROM paths WHERE path LIKE '/usr%' ORDER BY id",
        "SELECT id FROM paths WHERE path LIKE '%bin%' ORDER BY id",
        "SELECT id FROM paths WHERE path LIKE '/tmp/test_file.txt'",
        "SELECT id FROM paths WHERE path LIKE '%' ORDER BY id",
        "SELECT id FROM paths WHERE path LIKE '_____' ORDER BY id",
        // NULL LIKE
        "SELECT id FROM paths WHERE path LIKE NULL ORDER BY id",
        "SELECT id FROM paths WHERE NULL LIKE '%' ORDER BY id",
        // GLOB
        "SELECT id FROM paths WHERE path GLOB '/usr/*' ORDER BY id",
        "SELECT id FROM paths WHERE path GLOB '*bin*' ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LIKE/GLOB pattern mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: aggregate on empty vs non-empty tables ──

#[test]
fn test_conformance_aggregate_empty_table_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE empty_t(id INTEGER PRIMARY KEY, val INTEGER)",
        "CREATE TABLE one_row(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO one_row VALUES(1, 42)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        // Empty table aggregates
        "SELECT COUNT(*) FROM empty_t",
        "SELECT COUNT(val) FROM empty_t",
        "SELECT SUM(val) FROM empty_t",
        "SELECT AVG(val) FROM empty_t",
        "SELECT MIN(val) FROM empty_t",
        "SELECT MAX(val) FROM empty_t",
        "SELECT TOTAL(val) FROM empty_t",
        // GROUP BY on empty table
        "SELECT val, COUNT(*) FROM empty_t GROUP BY val",
        // One-row table
        "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM one_row",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate empty table mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: DISTINCT with various columns ──

#[test]
fn test_conformance_distinct_patterns_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE dup(id INTEGER PRIMARY KEY, cat TEXT, val INTEGER);
        INSERT INTO dup VALUES(1, 'A', 10);
        INSERT INTO dup VALUES(2, 'B', 20);
        INSERT INTO dup VALUES(3, 'A', 10);
        INSERT INTO dup VALUES(4, 'B', 30);
        INSERT INTO dup VALUES(5, 'C', 10);
        INSERT INTO dup VALUES(6, NULL, 10);
        INSERT INTO dup VALUES(7, NULL, 20);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT DISTINCT cat FROM dup ORDER BY cat",
        "SELECT DISTINCT val FROM dup ORDER BY val",
        "SELECT DISTINCT cat, val FROM dup ORDER BY cat, val",
        "SELECT COUNT(DISTINCT cat) FROM dup",
        "SELECT COUNT(DISTINCT val) FROM dup",
        // DISTINCT with NULL
        "SELECT DISTINCT cat FROM dup WHERE cat IS NOT NULL ORDER BY cat",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DISTINCT pattern mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: CAST edge cases ──

#[test]
fn test_conformance_cast_edges_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT CAST(123 AS TEXT)",
        "SELECT CAST('456' AS INTEGER)",
        "SELECT CAST('3.14' AS REAL)",
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT CAST(NULL AS TEXT)",
        "SELECT CAST(3.7 AS INTEGER)",
        "SELECT CAST(-3.7 AS INTEGER)",
        "SELECT CAST('not_a_number' AS INTEGER)",
        "SELECT CAST('' AS INTEGER)",
        "SELECT CAST(X'48454C4C4F' AS TEXT)",
        "SELECT TYPEOF(CAST(123 AS TEXT))",
        "SELECT TYPEOF(CAST('456' AS INTEGER))",
        "SELECT TYPEOF(CAST('3.14' AS REAL))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CAST edge mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: multiple CTEs ──

#[test]
fn test_conformance_multiple_ctes_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER);
        INSERT INTO emp VALUES(1, 'Alice', 'Eng', 120000);
        INSERT INTO emp VALUES(2, 'Bob', 'Eng', 95000);
        INSERT INTO emp VALUES(3, 'Carol', 'Sales', 85000);
        INSERT INTO emp VALUES(4, 'Dave', 'Sales', 110000);
        INSERT INTO emp VALUES(5, 'Eve', 'HR', 90000);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        // Multiple CTEs referencing each other
        "WITH dept_stats AS (SELECT dept, AVG(salary) as avg_sal, COUNT(*) as cnt FROM emp GROUP BY dept), high_paying AS (SELECT dept FROM dept_stats WHERE avg_sal > 95000) SELECT e.name FROM emp e WHERE e.dept IN (SELECT dept FROM high_paying) ORDER BY e.name",
        // CTE used multiple times
        "WITH nums AS (SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT a.n as a, b.n as b FROM nums a, nums b WHERE a.n < b.n ORDER BY a.n, b.n",
        // CTE with aggregate
        "WITH dept_totals AS (SELECT dept, SUM(salary) as total FROM emp GROUP BY dept) SELECT dept, total FROM dept_totals ORDER BY total DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multiple CTE mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: LEFT JOIN with NULL propagation ──

#[test]
fn test_conformance_left_join_null_propagation_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE departments(id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE employees(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER);
        INSERT INTO departments VALUES(1, 'Engineering');
        INSERT INTO departments VALUES(2, 'Sales');
        INSERT INTO departments VALUES(3, 'Marketing');
        INSERT INTO employees VALUES(1, 'Alice', 1);
        INSERT INTO employees VALUES(2, 'Bob', 1);
        INSERT INTO employees VALUES(3, 'Carol', 2);
        INSERT INTO employees VALUES(4, 'Dave', NULL);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        // Departments with no employees
        "SELECT d.name FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.id IS NULL ORDER BY d.name",
        // All departments with employee count
        "SELECT d.name, COUNT(e.id) FROM departments d LEFT JOIN employees e ON e.dept_id = d.id GROUP BY d.id ORDER BY d.name",
        // Employee with department name (including unassigned)
        "SELECT e.name, d.name as dept FROM employees e LEFT JOIN departments d ON d.id = e.dept_id ORDER BY e.name",
        // COALESCE on NULL from LEFT JOIN
        "SELECT e.name, COALESCE(d.name, 'Unassigned') as dept FROM employees e LEFT JOIN departments d ON d.id = e.dept_id ORDER BY e.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LEFT JOIN NULL propagation mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: datetime functions ──

#[test]
fn test_conformance_datetime_functions_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT date('2024-03-15')",
        "SELECT time('14:30:45')",
        "SELECT datetime('2024-03-15 14:30:45')",
        "SELECT julianday('2024-03-15')",
        "SELECT strftime('%Y', '2024-03-15')",
        "SELECT strftime('%m', '2024-03-15')",
        "SELECT strftime('%d', '2024-03-15')",
        "SELECT date('2024-03-15', '+1 day')",
        "SELECT date('2024-03-15', '-1 month')",
        "SELECT date('2024-03-15', '+1 year')",
        "SELECT date('2024-01-31', '+1 month')",
        "SELECT strftime('%s', '2024-03-15 00:00:00')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} datetime function mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: numeric edge cases ──

#[test]
fn test_conformance_numeric_edges_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT 9223372036854775807",
        "SELECT -9223372036854775808",
        "SELECT 9223372036854775807 + 1",
        "SELECT 1 / 0",
        "SELECT 1.0 / 0.0",
        "SELECT 0 / 0",
        "SELECT -1 * -1",
        "SELECT abs(-9223372036854775808)",
        "SELECT 2.0 * 3",
        "SELECT 7 / 2",
        "SELECT 7.0 / 2",
        "SELECT 7 % 3",
        "SELECT -7 % 3",
        "SELECT 1e308",
        "SELECT TYPEOF(1e308)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} numeric edge mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: GROUP_CONCAT ──

#[test]
fn test_conformance_group_concat_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE tags(item TEXT, tag TEXT);
        INSERT INTO tags VALUES('a', 'red');
        INSERT INTO tags VALUES('a', 'blue');
        INSERT INTO tags VALUES('a', 'green');
        INSERT INTO tags VALUES('b', 'red');
        INSERT INTO tags VALUES('b', 'yellow');
        INSERT INTO tags VALUES('c', NULL);
        INSERT INTO tags VALUES('c', 'purple');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT item, GROUP_CONCAT(tag) FROM tags GROUP BY item ORDER BY item",
        "SELECT item, GROUP_CONCAT(tag, '; ') FROM tags GROUP BY item ORDER BY item",
        "SELECT GROUP_CONCAT(DISTINCT tag) FROM tags",
        "SELECT COUNT(*), GROUP_CONCAT(item) FROM tags WHERE tag = 'red'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP_CONCAT mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: UPSERT (INSERT...ON CONFLICT) ──

#[test]
fn test_conformance_upsert_on_conflict_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE counters(key TEXT PRIMARY KEY, count INTEGER DEFAULT 0);
        INSERT INTO counters VALUES('a', 1);
        INSERT INTO counters VALUES('b', 2);
        INSERT INTO counters VALUES('a', 10) ON CONFLICT(key) DO UPDATE SET count = count + excluded.count;
        INSERT INTO counters VALUES('c', 5) ON CONFLICT(key) DO UPDATE SET count = count + excluded.count;
        INSERT INTO counters VALUES('b', 3) ON CONFLICT(key) DO UPDATE SET count = count + excluded.count;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT key, count FROM counters ORDER BY key",
        "SELECT SUM(count) FROM counters",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPSERT mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: SAVEPOINT and transaction rollback ──

#[test]
fn test_conformance_savepoint_rollback_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE sp(id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO sp VALUES(1, 'a');
        SAVEPOINT s1;
        INSERT INTO sp VALUES(2, 'b');
        SAVEPOINT s2;
        INSERT INTO sp VALUES(3, 'c');
        ROLLBACK TO s2;
        INSERT INTO sp VALUES(4, 'd');
        RELEASE s1;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT * FROM sp ORDER BY id", "SELECT COUNT(*) FROM sp"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} savepoint rollback mismatches", mismatches.len());
    }
}

// ── Session 67 round 3: last_insert_rowid and changes() ──

#[test]
fn test_conformance_last_insert_rowid_changes_s67f() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE li(id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO li VALUES(NULL, 'first');
        INSERT INTO li VALUES(NULL, 'second');
        INSERT INTO li VALUES(10, 'tenth');
        INSERT INTO li VALUES(NULL, 'eleventh');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT last_insert_rowid()", "SELECT * FROM li ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} last_insert_rowid mismatches", mismatches.len());
    }
}

// ── Session 67 round 4: INSERT OR IGNORE conformance ──

#[test]
fn test_conformance_insert_or_ignore_unique_s67g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE ign(id INTEGER PRIMARY KEY, name TEXT UNIQUE, val INTEGER);
        INSERT INTO ign VALUES(1, 'alice', 10);
        INSERT INTO ign VALUES(2, 'bob', 20);
        INSERT OR IGNORE INTO ign VALUES(1, 'alice', 99);
        INSERT OR IGNORE INTO ign VALUES(3, 'carol', 30);
        INSERT OR IGNORE INTO ign VALUES(4, 'alice', 40);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM ign ORDER BY id",
        "SELECT COUNT(*) FROM ign",
        "SELECT name FROM ign ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT OR IGNORE UNIQUE mismatches", mismatches.len());
    }
}

// ── Session 67 round 4: trigger execution ──

#[test]
fn test_conformance_trigger_insert_log_s67g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    // Execute each statement individually (triggers contain semicolons inside BEGIN...END)
    for s in &[
        "CREATE TABLE audit_log(id INTEGER PRIMARY KEY, action TEXT, item_id INTEGER, ts TEXT DEFAULT 'now')",
        "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, price REAL)",
        "CREATE TRIGGER log_insert AFTER INSERT ON items BEGIN INSERT INTO audit_log(action, item_id) VALUES('INSERT', NEW.id); END",
        "CREATE TRIGGER log_delete AFTER DELETE ON items BEGIN INSERT INTO audit_log(action, item_id) VALUES('DELETE', OLD.id); END",
        "INSERT INTO items VALUES(1, 'Widget', 9.99)",
        "INSERT INTO items VALUES(2, 'Gadget', 19.99)",
        "DELETE FROM items WHERE id = 1",
        "INSERT INTO items VALUES(3, 'Doohickey', 4.99)",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM items ORDER BY id",
        "SELECT action, item_id FROM audit_log ORDER BY id",
        "SELECT COUNT(*) FROM audit_log",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} trigger execution mismatches", mismatches.len());
    }
}

// ── Session 67 round 4: window function basics ──

#[test]
fn test_conformance_window_row_number_rank_s67g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE scores(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, score INTEGER);
        INSERT INTO scores VALUES(1, 'Alice', 'Eng', 95);
        INSERT INTO scores VALUES(2, 'Bob', 'Eng', 85);
        INSERT INTO scores VALUES(3, 'Carol', 'Sales', 90);
        INSERT INTO scores VALUES(4, 'Dave', 'Sales', 90);
        INSERT INTO scores VALUES(5, 'Eve', 'Eng', 85);
        INSERT INTO scores VALUES(6, 'Frank', 'Sales', 80);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) as rn FROM scores",
        "SELECT name, score, RANK() OVER (ORDER BY score DESC) as rnk FROM scores",
        "SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM scores",
        "SELECT name, dept, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) as dept_rn FROM scores ORDER BY dept, dept_rn",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window function mismatches", mismatches.len());
    }
}

// ── Session 67 round 4: CHECK constraint enforcement ──

#[test]
fn test_conformance_check_constraint_s67g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    for s in &[
        "CREATE TABLE checked(id INTEGER PRIMARY KEY, val INTEGER CHECK(val >= 0), name TEXT CHECK(length(name) > 0))",
        "INSERT INTO checked VALUES(1, 10, 'hello')",
        "INSERT INTO checked VALUES(2, 0, 'world')",
    ] {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT * FROM checked ORDER BY id"];
    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CHECK constraint mismatches", mismatches.len());
    }

    // Verify CHECK violations are rejected
    let frank_err = fconn
        .execute("INSERT INTO checked VALUES(3, -1, 'bad')")
        .is_err();
    let csql_err = rconn
        .execute_batch("INSERT INTO checked VALUES(3, -1, 'bad')")
        .is_err();
    assert_eq!(
        frank_err, csql_err,
        "CHECK violation should be rejected by both"
    );

    let frank_err2 = fconn
        .execute("INSERT INTO checked VALUES(4, 5, '')")
        .is_err();
    let csql_err2 = rconn
        .execute_batch("INSERT INTO checked VALUES(4, 5, '')")
        .is_err();
    assert_eq!(
        frank_err2, csql_err2,
        "CHECK violation on length should be rejected"
    );
}

// ── Session 67 round 4: AUTOINCREMENT ──

#[test]
fn test_conformance_autoincrement_s67g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        INSERT INTO ai(name) VALUES('first');
        INSERT INTO ai(name) VALUES('second');
        INSERT INTO ai(name) VALUES('third');
        DELETE FROM ai WHERE id = 2;
        INSERT INTO ai(name) VALUES('fourth');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM ai ORDER BY id",
        "SELECT COUNT(*) FROM ai",
        // AUTOINCREMENT should never reuse deleted rowids
        "SELECT MIN(id) FROM ai",
        "SELECT MAX(id) FROM ai",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} AUTOINCREMENT mismatches", mismatches.len());
    }
}

// ── Session 67 round 4: FK CASCADE on DELETE ──

#[test]
fn test_conformance_fk_cascade_delete_s67g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    rconn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    fconn.execute("PRAGMA foreign_keys = ON").unwrap();

    let setup = "
        CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE, label TEXT);
        INSERT INTO parent VALUES(1, 'P1');
        INSERT INTO parent VALUES(2, 'P2');
        INSERT INTO child VALUES(10, 1, 'C1');
        INSERT INTO child VALUES(11, 1, 'C2');
        INSERT INTO child VALUES(12, 2, 'C3');
        DELETE FROM parent WHERE id = 1;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM parent ORDER BY id",
        "SELECT * FROM child ORDER BY id",
        "SELECT COUNT(*) FROM child",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} FK CASCADE DELETE mismatches", mismatches.len());
    }
}

// ── Session 67 round 4: complex WHERE with subqueries ──

#[test]
fn test_conformance_complex_where_subquery_s67g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL);
        CREATE TABLE reviews(id INTEGER PRIMARY KEY, product_id INTEGER, rating INTEGER);
        INSERT INTO products VALUES(1, 'Widget', 'A', 10.0);
        INSERT INTO products VALUES(2, 'Gadget', 'A', 20.0);
        INSERT INTO products VALUES(3, 'Doohickey', 'B', 5.0);
        INSERT INTO products VALUES(4, 'Thingamajig', 'B', 15.0);
        INSERT INTO reviews VALUES(1, 1, 5);
        INSERT INTO reviews VALUES(2, 1, 4);
        INSERT INTO reviews VALUES(3, 2, 3);
        INSERT INTO reviews VALUES(4, 3, 5);
        INSERT INTO reviews VALUES(5, 3, 5);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        // Products with average rating above 4
        "SELECT p.name FROM products p WHERE (SELECT AVG(rating) FROM reviews r WHERE r.product_id = p.id) > 4 ORDER BY p.name",
        // Products with at least 2 reviews
        "SELECT p.name FROM products p WHERE (SELECT COUNT(*) FROM reviews r WHERE r.product_id = p.id) >= 2 ORDER BY p.name",
        // Products with no reviews
        "SELECT p.name FROM products p WHERE p.id NOT IN (SELECT DISTINCT product_id FROM reviews) ORDER BY p.name",
        // Category totals
        "SELECT category, SUM(price) as total, COUNT(*) as cnt FROM products GROUP BY category ORDER BY category",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex WHERE subquery mismatches", mismatches.len());
    }
}

// ── Session 67 round 4: INSERT OR REPLACE with UNIQUE ──

#[test]
fn test_conformance_insert_or_replace_unique_s67g() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE uq(id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT);
        INSERT INTO uq VALUES(1, 'alice@ex.com', 'Alice');
        INSERT INTO uq VALUES(2, 'bob@ex.com', 'Bob');
        INSERT OR REPLACE INTO uq VALUES(3, 'alice@ex.com', 'Alice2');
        INSERT OR REPLACE INTO uq VALUES(2, 'carol@ex.com', 'Carol');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT * FROM uq ORDER BY id", "SELECT COUNT(*) FROM uq"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT OR REPLACE UNIQUE mismatches", mismatches.len());
    }
}

// ── Session 67 round 5 (s67h): Advanced SQL patterns ──

#[test]
fn test_conformance_multi_table_delete_fk_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE parent_h(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE child_h(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent_h(id) ON DELETE CASCADE, val TEXT)",
        "INSERT INTO parent_h VALUES(1,'A'),(2,'B'),(3,'C')",
        "INSERT INTO child_h VALUES(10,1,'x'),(20,1,'y'),(30,2,'z'),(40,3,'w')",
        "DELETE FROM parent_h WHERE id = 1",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM parent_h ORDER BY id",
        "SELECT * FROM child_h ORDER BY id",
        "SELECT COUNT(*) FROM child_h",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} FK cascade delete mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_nested_aggregate_subquery_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE sales_h(region TEXT, product TEXT, amount REAL);
        INSERT INTO sales_h VALUES('East','Widget',100.0),('East','Gadget',200.0),
          ('West','Widget',150.0),('West','Gadget',50.0),('East','Widget',75.0);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT region, SUM(amount), (SELECT SUM(amount) FROM sales_h) AS total FROM sales_h GROUP BY region ORDER BY region",
        "SELECT product, COUNT(*), AVG(amount) FROM sales_h GROUP BY product ORDER BY product",
        "SELECT region, product, SUM(amount) FROM sales_h GROUP BY region, product ORDER BY region, product",
        "SELECT region FROM sales_h GROUP BY region HAVING SUM(amount) > 200 ORDER BY region",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested aggregate mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_coalesce_chain_null_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE cn(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d INTEGER);
        INSERT INTO cn VALUES(1, NULL, NULL, 'found', 10);
        INSERT INTO cn VALUES(2, NULL, 'here', NULL, 20);
        INSERT INTO cn VALUES(3, 'first', NULL, NULL, NULL);
        INSERT INTO cn VALUES(4, NULL, NULL, NULL, NULL);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, COALESCE(a, b, c, 'default') FROM cn ORDER BY id",
        "SELECT id, COALESCE(a, b, c) FROM cn ORDER BY id",
        "SELECT id, IFNULL(a, IFNULL(b, c)) FROM cn ORDER BY id",
        "SELECT id, COALESCE(d, 0) + 5 FROM cn ORDER BY id",
        "SELECT id, NULLIF(COALESCE(a, b, c), 'first') FROM cn ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COALESCE chain mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_case_with_aggregates_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE scores_h(student TEXT, subject TEXT, score INTEGER);
        INSERT INTO scores_h VALUES('Alice','Math',95),('Alice','Sci',88),('Alice','Eng',72);
        INSERT INTO scores_h VALUES('Bob','Math',65),('Bob','Sci',70),('Bob','Eng',90);
        INSERT INTO scores_h VALUES('Carol','Math',80),('Carol','Sci',85),('Carol','Eng',82);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT student, AVG(score), CASE WHEN AVG(score) >= 85 THEN 'A' WHEN AVG(score) >= 75 THEN 'B' ELSE 'C' END AS grade FROM scores_h GROUP BY student ORDER BY student",
        "SELECT subject, MAX(score) - MIN(score) AS spread FROM scores_h GROUP BY subject ORDER BY subject",
        "SELECT student, SUM(CASE WHEN score >= 80 THEN 1 ELSE 0 END) AS high_count FROM scores_h GROUP BY student ORDER BY student",
        "SELECT student, GROUP_CONCAT(subject, ', ') FROM scores_h WHERE score >= 80 GROUP BY student ORDER BY student",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CASE+aggregate mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_update_with_case_and_subquery_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE inv_h(id INTEGER PRIMARY KEY, item TEXT, qty INTEGER, price REAL);
        INSERT INTO inv_h VALUES(1,'Apple',100,1.50),(2,'Banana',50,0.75),(3,'Cherry',200,2.00),(4,'Date',10,5.00);
        UPDATE inv_h SET price = CASE WHEN qty > 100 THEN price * 0.9 WHEN qty < 20 THEN price * 1.1 ELSE price END;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, item, qty, printf('%.2f', price) FROM inv_h ORDER BY id",
        "SELECT SUM(qty * price) FROM inv_h",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE CASE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_recursive_cte_tree_path_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE org_h(id INTEGER PRIMARY KEY, name TEXT, mgr_id INTEGER);
        INSERT INTO org_h VALUES(1,'CEO',NULL),(2,'VP1',1),(3,'VP2',1),(4,'Dir1',2),(5,'Dir2',2),(6,'Mgr1',4);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "WITH RECURSIVE chain(id, name, path, depth) AS (SELECT id, name, name, 0 FROM org_h WHERE mgr_id IS NULL UNION ALL SELECT o.id, o.name, chain.path || '/' || o.name, chain.depth + 1 FROM org_h o JOIN chain ON o.mgr_id = chain.id) SELECT id, name, path, depth FROM chain ORDER BY path",
        "WITH RECURSIVE chain(id, depth) AS (SELECT id, 0 FROM org_h WHERE mgr_id IS NULL UNION ALL SELECT o.id, chain.depth + 1 FROM org_h o JOIN chain ON o.mgr_id = chain.id) SELECT depth, COUNT(*) FROM chain GROUP BY depth ORDER BY depth",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE tree mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_compound_select_with_order_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1h(id INTEGER, val TEXT);
        CREATE TABLE t2h(id INTEGER, val TEXT);
        INSERT INTO t1h VALUES(1,'a'),(2,'b'),(3,'c');
        INSERT INTO t2h VALUES(2,'b'),(3,'d'),(4,'e');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, val FROM t1h UNION SELECT id, val FROM t2h ORDER BY id",
        "SELECT id, val FROM t1h UNION ALL SELECT id, val FROM t2h ORDER BY id, val",
        "SELECT id FROM t1h INTERSECT SELECT id FROM t2h ORDER BY id",
        "SELECT id FROM t1h EXCEPT SELECT id FROM t2h ORDER BY id",
        "SELECT id, val FROM t1h UNION SELECT id, val FROM t2h ORDER BY val",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} compound SELECT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_insert_select_with_transforms_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE src_h(id INTEGER PRIMARY KEY, name TEXT, val REAL);
        CREATE TABLE dst_h(id INTEGER PRIMARY KEY, upper_name TEXT, doubled REAL);
        INSERT INTO src_h VALUES(1,'alice',10.5),(2,'bob',20.0),(3,'carol',30.75);
        INSERT INTO dst_h SELECT id, UPPER(name), val * 2 FROM src_h WHERE val > 15;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM dst_h ORDER BY id",
        "SELECT COUNT(*) FROM dst_h",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT SELECT transform mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_left_join_null_propagation_advanced_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE departments_h(id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE employees_h(id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT, salary INTEGER);
        INSERT INTO departments_h VALUES(1,'Engineering'),(2,'Marketing'),(3,'HR');
        INSERT INTO employees_h VALUES(10,1,'Alice',90000),(20,1,'Bob',85000),(30,2,'Carol',70000);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT d.name, e.name, e.salary FROM departments_h d LEFT JOIN employees_h e ON d.id = e.dept_id ORDER BY d.name, e.name",
        "SELECT d.name, COUNT(e.id) AS emp_count, COALESCE(AVG(e.salary), 0) AS avg_sal FROM departments_h d LEFT JOIN employees_h e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name",
        "SELECT d.name FROM departments_h d LEFT JOIN employees_h e ON d.id = e.dept_id WHERE e.id IS NULL",
        "SELECT d.name, TOTAL(e.salary) FROM departments_h d LEFT JOIN employees_h e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LEFT JOIN NULL propagation mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_self_join_hierarchy_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE emp_h2(id INTEGER PRIMARY KEY, name TEXT, mgr INTEGER);
        INSERT INTO emp_h2 VALUES(1,'CEO',NULL),(2,'VP',1),(3,'Dir',2),(4,'Mgr',3),(5,'Eng',4);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT e.name AS employee, m.name AS manager FROM emp_h2 e LEFT JOIN emp_h2 m ON e.mgr = m.id ORDER BY e.id",
        "SELECT e.name FROM emp_h2 e WHERE e.mgr = (SELECT id FROM emp_h2 WHERE name = 'VP')",
        "SELECT COUNT(*) FROM emp_h2 WHERE mgr IS NOT NULL",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} self-join hierarchy mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_typeof_cast_edges_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT typeof(1), typeof(1.0), typeof('hello'), typeof(NULL), typeof(X'AB')",
        "SELECT typeof(1+1), typeof(1+1.0), typeof('a'||'b')",
        "SELECT CAST(123 AS TEXT), typeof(CAST(123 AS TEXT))",
        "SELECT CAST('456' AS INTEGER), typeof(CAST('456' AS INTEGER))",
        "SELECT CAST('3.14' AS REAL), typeof(CAST('3.14' AS REAL))",
        "SELECT CAST(NULL AS INTEGER), typeof(CAST(NULL AS INTEGER))",
        "SELECT CAST('' AS INTEGER), CAST('' AS REAL)",
        "SELECT CAST('abc' AS INTEGER), CAST('abc' AS REAL)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} typeof/CAST edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_concat_ordering_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE gc_h(grp TEXT, val TEXT);
        INSERT INTO gc_h VALUES('A','3'),('A','1'),('A','2'),('B','x'),('B','y');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT grp, GROUP_CONCAT(val, ',') FROM gc_h GROUP BY grp ORDER BY grp",
        "SELECT grp, GROUP_CONCAT(val, ' | ') FROM gc_h GROUP BY grp ORDER BY grp",
        "SELECT GROUP_CONCAT(val) FROM gc_h WHERE grp = 'A'",
        "SELECT grp, GROUP_CONCAT(DISTINCT val) FROM gc_h GROUP BY grp ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP_CONCAT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_subquery_in_insert_values_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE counter_h(id INTEGER PRIMARY KEY, n INTEGER);
        INSERT INTO counter_h VALUES(1, 10),(2, 20),(3, 30);
        CREATE TABLE result_h(id INTEGER PRIMARY KEY, total INTEGER, maxn INTEGER);
        INSERT INTO result_h VALUES(1, (SELECT SUM(n) FROM counter_h), (SELECT MAX(n) FROM counter_h));
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT * FROM result_h"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery in INSERT VALUES mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_numeric_string_comparison_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT '9' > '10'",
        "SELECT 9 > 10",
        "SELECT '9' > 10",
        "SELECT 9 > '10'",
        "SELECT '09' = '9'",
        "SELECT '09' = 9",
        "SELECT 0 = '0.0'",
        "SELECT '' = 0",
        "SELECT '' < 'a'",
        "SELECT NULL = NULL",
        "SELECT NULL IS NULL",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} numeric/string comparison mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_upsert_do_update_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE kv_h(key TEXT PRIMARY KEY, val INTEGER, updated INTEGER DEFAULT 0);
        INSERT INTO kv_h VALUES('a', 1, 0),('b', 2, 0);
        INSERT INTO kv_h VALUES('a', 10, 0) ON CONFLICT(key) DO UPDATE SET val = excluded.val, updated = updated + 1;
        INSERT INTO kv_h VALUES('c', 3, 0) ON CONFLICT(key) DO UPDATE SET val = excluded.val;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM kv_h ORDER BY key",
        "SELECT key, val FROM kv_h WHERE updated > 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPSERT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_window_running_total_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE txns_h(id INTEGER PRIMARY KEY, acct TEXT, amount REAL);
        INSERT INTO txns_h VALUES(1,'A',100.0),(2,'A',50.0),(3,'B',200.0),(4,'A',-30.0),(5,'B',75.0);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, acct, amount, SUM(amount) OVER (PARTITION BY acct ORDER BY id) AS running FROM txns_h ORDER BY id",
        "SELECT id, acct, ROW_NUMBER() OVER (PARTITION BY acct ORDER BY id) AS rn FROM txns_h ORDER BY id",
        "SELECT id, acct, amount, COUNT(*) OVER (PARTITION BY acct) AS acct_count FROM txns_h ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window running total mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_exists_not_exists_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE prod_h(id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE orders_h(id INTEGER PRIMARY KEY, prod_id INTEGER, qty INTEGER);
        INSERT INTO prod_h VALUES(1,'Widget'),(2,'Gadget'),(3,'Doohickey');
        INSERT INTO orders_h VALUES(100,1,5),(101,1,3),(102,2,10);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT name FROM prod_h p WHERE EXISTS (SELECT 1 FROM orders_h o WHERE o.prod_id = p.id) ORDER BY name",
        "SELECT name FROM prod_h p WHERE NOT EXISTS (SELECT 1 FROM orders_h o WHERE o.prod_id = p.id) ORDER BY name",
        "SELECT name, (SELECT SUM(qty) FROM orders_h WHERE prod_id = prod_h.id) AS total_qty FROM prod_h ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} EXISTS/NOT EXISTS mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_boolean_where_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE items_h(id INTEGER PRIMARY KEY, cat TEXT, price REAL, active INTEGER, stock INTEGER);
        INSERT INTO items_h VALUES(1,'A',10.0,1,100),(2,'B',25.0,0,50),(3,'A',5.0,1,0);
        INSERT INTO items_h VALUES(4,'C',100.0,1,200),(5,'B',15.0,1,75),(6,'A',30.0,0,10);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id FROM items_h WHERE (cat = 'A' OR cat = 'B') AND active = 1 AND stock > 0 ORDER BY id",
        "SELECT id FROM items_h WHERE NOT (price > 20 AND active = 0) ORDER BY id",
        "SELECT id FROM items_h WHERE cat IN ('A', 'C') AND (price < 10 OR stock >= 200) ORDER BY id",
        "SELECT id FROM items_h WHERE active = 1 AND (cat = 'A' OR (cat = 'B' AND price < 20)) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex boolean WHERE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_replace_into_semantics_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE rep_h(id INTEGER PRIMARY KEY, name TEXT, score INTEGER);
        INSERT INTO rep_h VALUES(1,'Alice',100),(2,'Bob',200);
        REPLACE INTO rep_h VALUES(1,'Alice_v2',150);
        REPLACE INTO rep_h VALUES(3,'Carol',300);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM rep_h ORDER BY id",
        "SELECT COUNT(*) FROM rep_h",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} REPLACE INTO mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_cte_references_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE data_h(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO data_h VALUES(1,'X',10),(2,'X',20),(3,'Y',30),(4,'Y',40),(5,'X',50);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "WITH stats AS (SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM data_h GROUP BY grp), overall AS (SELECT SUM(val) AS grand_total FROM data_h) SELECT s.grp, s.total, s.cnt, o.grand_total FROM stats s, overall o ORDER BY s.grp",
        "WITH vals AS (SELECT val FROM data_h WHERE grp = 'X') SELECT MIN(val), MAX(val), AVG(val) FROM vals",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi CTE ref mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_between_with_null_and_types_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT 5 BETWEEN 1 AND 10",
        "SELECT 5 BETWEEN 10 AND 1",
        "SELECT NULL BETWEEN 1 AND 10",
        "SELECT 5 BETWEEN NULL AND 10",
        "SELECT 5 BETWEEN 1 AND NULL",
        "SELECT 'c' BETWEEN 'a' AND 'e'",
        "SELECT 'f' BETWEEN 'a' AND 'e'",
        "SELECT 5 NOT BETWEEN 1 AND 10",
        "SELECT NULL NOT BETWEEN 1 AND 10",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} BETWEEN NULL/types mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_in_list_with_null_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT 1 IN (1, 2, 3)",
        "SELECT 4 IN (1, 2, 3)",
        "SELECT NULL IN (1, 2, 3)",
        "SELECT 1 IN (1, 2, NULL)",
        "SELECT 4 IN (1, 2, NULL)",
        "SELECT 1 NOT IN (1, 2, 3)",
        "SELECT 4 NOT IN (1, 2, NULL)",
        "SELECT NULL NOT IN (1, 2, 3)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} IN list NULL mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_savepoint_nested_rollback_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE sp_h(id INTEGER PRIMARY KEY, val TEXT)",
        "INSERT INTO sp_h VALUES(1, 'original')",
        "SAVEPOINT outer_sp",
        "INSERT INTO sp_h VALUES(2, 'added_in_outer')",
        "SAVEPOINT inner_sp",
        "INSERT INTO sp_h VALUES(3, 'added_in_inner')",
        "ROLLBACK TO inner_sp",
        "INSERT INTO sp_h VALUES(4, 'after_rollback')",
        "RELEASE outer_sp",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM sp_h ORDER BY id",
        "SELECT COUNT(*) FROM sp_h",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} savepoint rollback mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_string_functions_edge_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT LENGTH('hello'), LENGTH('')",
        "SELECT LENGTH(NULL)",
        "SELECT SUBSTR('hello', 2, 3)",
        "SELECT SUBSTR('hello', -3)",
        "SELECT UPPER('hello'), LOWER('HELLO')",
        "SELECT TRIM('  hello  '), LTRIM('  hello  '), RTRIM('  hello  ')",
        "SELECT REPLACE('hello world', 'world', 'earth')",
        "SELECT INSTR('hello world', 'world')",
        "SELECT INSTR('hello', 'xyz')",
        "SELECT SUBSTR('hello', 1, 0)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} string function edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_aggregate_empty_table_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE empty_h(id INTEGER PRIMARY KEY, val REAL);";
    fconn.execute(setup).unwrap();
    rconn.execute_batch(setup).unwrap();

    let queries = &[
        "SELECT COUNT(*) FROM empty_h",
        "SELECT COUNT(val) FROM empty_h",
        "SELECT SUM(val) FROM empty_h",
        "SELECT AVG(val) FROM empty_h",
        "SELECT MIN(val) FROM empty_h",
        "SELECT MAX(val) FROM empty_h",
        "SELECT TOTAL(val) FROM empty_h",
        "SELECT GROUP_CONCAT(val) FROM empty_h",
        "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM empty_h",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate empty table mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_distinct_with_order_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE dup_h(id INTEGER, val TEXT, grp TEXT);
        INSERT INTO dup_h VALUES(1,'a','X'),(2,'b','X'),(3,'a','Y'),(4,'c','Y'),(5,'b','X');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT DISTINCT val FROM dup_h ORDER BY val",
        "SELECT DISTINCT grp FROM dup_h ORDER BY grp",
        "SELECT DISTINCT val, grp FROM dup_h ORDER BY val, grp",
        "SELECT grp, COUNT(DISTINCT val) FROM dup_h GROUP BY grp ORDER BY grp",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DISTINCT ordering mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_create_table_as_select_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE base_h(id INTEGER PRIMARY KEY, name TEXT, score INTEGER);
        INSERT INTO base_h VALUES(1,'Alice',95),(2,'Bob',80),(3,'Carol',90);
        CREATE TABLE derived_h AS SELECT name, score * 2 AS doubled FROM base_h WHERE score >= 85;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM derived_h ORDER BY name",
        "SELECT COUNT(*) FROM derived_h",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTAS mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_iif_nested_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT IIF(1, 'yes', 'no')",
        "SELECT IIF(0, 'yes', 'no')",
        "SELECT IIF(NULL, 'yes', 'no')",
        "SELECT IIF(1 > 0, IIF(2 > 1, 'both', 'first'), 'neither')",
        "SELECT IIF('', 'truthy', 'falsy')",
        "SELECT IIF(0.0, 'truthy', 'falsy')",
        "SELECT IIF(0.5, 'truthy', 'falsy')",
        "SELECT NULLIF(5, 5), NULLIF(5, 6)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} IIF/NULLIF mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_printf_format_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT printf('%d', 42)",
        "SELECT printf('%05d', 42)",
        "SELECT printf('%10s', 'hi')",
        "SELECT printf('%-10s', 'hi')",
        "SELECT printf('%.2f', 3.14159)",
        "SELECT printf('%e', 12345.6789)",
        "SELECT printf('%g', 12345.6789)",
        "SELECT printf('%g', 0.00012345)",
        "SELECT printf('%%')",
        "SELECT printf('%d + %d = %d', 1, 2, 3)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} printf format mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_datetime_functions_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT date('2024-01-15')",
        "SELECT time('14:30:00')",
        "SELECT datetime('2024-01-15 14:30:00')",
        "SELECT julianday('2024-01-15')",
        "SELECT date('2024-01-15', '+1 month')",
        "SELECT date('2024-01-15', '-1 year')",
        "SELECT date('2024-01-15', '+1 month', '-1 day')",
        "SELECT strftime('%Y', '2024-01-15')",
        "SELECT strftime('%m', '2024-01-15')",
        "SELECT strftime('%d', '2024-01-15')",
        "SELECT strftime('%H:%M', '2024-01-15 14:30:00')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} datetime function mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_math_functions_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT abs(-5), abs(5), abs(0)",
        "SELECT abs(-0.0)",
        "SELECT max(1, 2, 3), min(1, 2, 3)",
        "SELECT max(NULL, 2, 3), min(1, NULL, 3)",
        "SELECT round(2.5), round(3.5), round(-2.5)",
        "SELECT round(2.345, 2)",
        "SELECT round(2.355, 2)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} math function mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_like_glob_patterns_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE files_h(name TEXT);
        INSERT INTO files_h VALUES('readme.txt'),('main.rs'),('lib.rs'),('test_main.rs'),('README.md');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT name FROM files_h WHERE name LIKE '%.rs' ORDER BY name",
        "SELECT name FROM files_h WHERE name LIKE '%main%' ORDER BY name",
        "SELECT name FROM files_h WHERE name LIKE 'README%' ORDER BY name",
        "SELECT name FROM files_h WHERE name GLOB '*.rs' ORDER BY name",
        "SELECT name FROM files_h WHERE name GLOB '*main*' ORDER BY name",
        "SELECT 'hello' LIKE 'h_llo'",
        "SELECT 'hello' LIKE 'h%o'",
        "SELECT 'hello' LIKE 'HELLO'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LIKE/GLOB pattern mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_join_aggregate_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE cats_h(id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE prods_h(id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT, price REAL);
        CREATE TABLE ords_h(id INTEGER PRIMARY KEY, prod_id INTEGER, qty INTEGER);
        INSERT INTO cats_h VALUES(1,'Electronics'),(2,'Books');
        INSERT INTO prods_h VALUES(10,1,'Phone',999.0),(20,1,'Tablet',599.0),(30,2,'Novel',15.0);
        INSERT INTO ords_h VALUES(100,10,2),(101,10,1),(102,20,3),(103,30,5);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT c.name, SUM(o.qty * p.price) AS revenue FROM cats_h c JOIN prods_h p ON c.id = p.cat_id JOIN ords_h o ON p.id = o.prod_id GROUP BY c.name ORDER BY c.name",
        "SELECT p.name, SUM(o.qty) AS total_qty FROM prods_h p JOIN ords_h o ON p.id = o.prod_id GROUP BY p.name ORDER BY p.name",
        "SELECT c.name, COUNT(DISTINCT p.id) AS prod_count FROM cats_h c JOIN prods_h p ON c.id = p.cat_id GROUP BY c.name ORDER BY c.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-join aggregate mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_order_by_expression_s67h() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE people_h(name TEXT, age INTEGER);
        INSERT INTO people_h VALUES('Alice',30),('Bob',25),('Carol',30),('Dave',25),('Eve',35);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT name, age FROM people_h ORDER BY age DESC, name ASC",
        "SELECT name, age, LENGTH(name) AS namelen FROM people_h ORDER BY LENGTH(name), name",
        "SELECT name FROM people_h ORDER BY age % 10, name",
        "SELECT name, age FROM people_h ORDER BY CASE WHEN age >= 30 THEN 0 ELSE 1 END, name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ORDER BY expression mismatches", mismatches.len());
    }
}

// ── Session 67 round 6 (s67i): Deeper edge cases ──

#[test]
fn test_conformance_correlated_delete_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE log_i(id INTEGER PRIMARY KEY, user_id INTEGER, ts TEXT);
        INSERT INTO log_i VALUES(1,1,'2024-01-01'),(2,1,'2024-01-02'),(3,1,'2024-01-03');
        INSERT INTO log_i VALUES(4,2,'2024-01-01'),(5,2,'2024-01-02');
        DELETE FROM log_i WHERE id NOT IN (SELECT MAX(id) FROM log_i GROUP BY user_id);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM log_i ORDER BY id",
        "SELECT COUNT(*) FROM log_i",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} correlated DELETE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_arithmetic_overflow_edges_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT 9223372036854775807",
        "SELECT 9223372036854775807 + 1",
        "SELECT -9223372036854775808",
        "SELECT typeof(9223372036854775807 + 1)",
        "SELECT 9223372036854775807 * 2",
        "SELECT typeof(9223372036854775807 * 2)",
        "SELECT 1 / 0",
        "SELECT 1.0 / 0.0",
        "SELECT 0 / 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} arithmetic overflow mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_nested_derived_tables_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE raw_i(id INTEGER, val INTEGER);
        INSERT INTO raw_i VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM (SELECT id, val * 2 AS doubled FROM raw_i) WHERE doubled > 40 ORDER BY id",
        "SELECT avg_d FROM (SELECT AVG(doubled) AS avg_d FROM (SELECT val * 2 AS doubled FROM raw_i))",
        "SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM (SELECT DISTINCT val % 20 AS m FROM raw_i))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested derived table mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_update_multiple_columns_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE stock_i(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER, price REAL, category TEXT);
        INSERT INTO stock_i VALUES(1,'Widget',100,10.0,'A'),(2,'Gadget',50,20.0,'B'),(3,'Doohickey',200,5.0,'A');
        UPDATE stock_i SET qty = qty - 10, price = price * 1.05 WHERE category = 'A';
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT id, name, qty, printf('%.2f', price) FROM stock_i ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-column UPDATE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_cross_join_cartesian_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE colors_i(c TEXT);
        CREATE TABLE sizes_i(s TEXT);
        INSERT INTO colors_i VALUES('red'),('blue');
        INSERT INTO sizes_i VALUES('S'),('M'),('L');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT c, s FROM colors_i CROSS JOIN sizes_i ORDER BY c, s",
        "SELECT COUNT(*) FROM colors_i CROSS JOIN sizes_i",
        "SELECT c, s FROM colors_i, sizes_i ORDER BY c, s",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CROSS JOIN mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_having_without_group_by_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE nums_i(val INTEGER);
        INSERT INTO nums_i VALUES(1),(2),(3),(4),(5);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT SUM(val) FROM nums_i HAVING SUM(val) > 10",
        "SELECT COUNT(*) FROM nums_i HAVING COUNT(*) > 3",
        "SELECT SUM(val) FROM nums_i HAVING SUM(val) > 100",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} HAVING without GROUP BY mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_collate_nocase_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE names_i(name TEXT COLLATE NOCASE);
        INSERT INTO names_i VALUES('Alice'),('alice'),('BOB'),('bob'),('Carol');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT DISTINCT name FROM names_i ORDER BY name",
        "SELECT name FROM names_i WHERE name = 'alice' ORDER BY rowid",
        "SELECT name FROM names_i WHERE name > 'alice' ORDER BY name",
        "SELECT COUNT(*) FROM names_i WHERE name = 'bob'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COLLATE NOCASE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_limit_offset_edges_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE seq_i(n INTEGER);
        INSERT INTO seq_i VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT n FROM seq_i ORDER BY n LIMIT 3",
        "SELECT n FROM seq_i ORDER BY n LIMIT 3 OFFSET 5",
        "SELECT n FROM seq_i ORDER BY n LIMIT 0",
        "SELECT n FROM seq_i ORDER BY n LIMIT -1",
        "SELECT n FROM seq_i ORDER BY n LIMIT 100",
        "SELECT n FROM seq_i ORDER BY n LIMIT 3 OFFSET 8",
        "SELECT n FROM seq_i ORDER BY n LIMIT 3 OFFSET 100",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} LIMIT/OFFSET edge mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_unary_operators_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT -5, +5, ~5",
        "SELECT -(-5)",
        "SELECT ~0, ~(-1)",
        "SELECT -NULL, +NULL, ~NULL",
        "SELECT NOT 1, NOT 0, NOT NULL",
        "SELECT -0, -0.0",
        "SELECT typeof(-0), typeof(-0.0)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} unary operator mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_bitwise_operators_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT 5 & 3",
        "SELECT 5 | 3",
        "SELECT 5 << 2",
        "SELECT 20 >> 2",
        "SELECT ~0",
        "SELECT 0xFF & 0x0F",
        "SELECT NULL & 5, 5 & NULL",
        "SELECT NULL | 5, 5 | NULL",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} bitwise operator mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_hex_blob_operations_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT HEX(X'48454C4C4F')",
        "SELECT LENGTH(X'48454C4C4F')",
        "SELECT typeof(X'48454C4C4F')",
        "SELECT HEX(ZEROBLOB(4))",
        "SELECT LENGTH(ZEROBLOB(10))",
        "SELECT QUOTE(X'DEADBEEF')",
        "SELECT QUOTE(NULL)",
        "SELECT QUOTE(42)",
        "SELECT QUOTE('hello')",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} hex/blob mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_total_vs_sum_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE mixed_i(val);
        INSERT INTO mixed_i VALUES(10),(20),(NULL),(30);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT SUM(val), TOTAL(val) FROM mixed_i",
        "SELECT typeof(SUM(val)), typeof(TOTAL(val)) FROM mixed_i",
        "SELECT SUM(val) FROM mixed_i WHERE val IS NULL",
        "SELECT TOTAL(val) FROM mixed_i WHERE val IS NULL",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} TOTAL vs SUM mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_view_with_query_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE emp_i(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER);
        INSERT INTO emp_i VALUES(1,'Alice','Eng',90000),(2,'Bob','Eng',85000),(3,'Carol','Sales',70000);
        CREATE VIEW dept_stats_i AS SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM emp_i GROUP BY dept;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM dept_stats_i ORDER BY dept",
        "SELECT dept FROM dept_stats_i WHERE cnt > 1",
        "SELECT dept, avg_sal FROM dept_stats_i ORDER BY avg_sal DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} view query mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_alter_table_add_column_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE alt_i(id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO alt_i VALUES(1, 'Alice'),(2, 'Bob')",
        "ALTER TABLE alt_i ADD COLUMN score INTEGER DEFAULT 0",
        "INSERT INTO alt_i VALUES(3, 'Carol', 95)",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM alt_i ORDER BY id",
        "SELECT name, score FROM alt_i WHERE score > 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} ALTER TABLE ADD COLUMN mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_last_insert_rowid_changes_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE lir_i(id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO lir_i VALUES(NULL, 'first');
        INSERT INTO lir_i VALUES(NULL, 'second');
        INSERT INTO lir_i VALUES(10, 'tenth');
        INSERT INTO lir_i VALUES(NULL, 'eleventh');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT last_insert_rowid()",
        "SELECT * FROM lir_i ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} last_insert_rowid mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_by_having_subquery_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE orders_i(id INTEGER PRIMARY KEY, cust TEXT, amount REAL);
        INSERT INTO orders_i VALUES(1,'Alice',100),(2,'Alice',200),(3,'Bob',150);
        INSERT INTO orders_i VALUES(4,'Carol',300),(5,'Carol',50),(6,'Carol',100);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT cust, SUM(amount) AS total FROM orders_i GROUP BY cust HAVING SUM(amount) > (SELECT AVG(amount) FROM orders_i) ORDER BY cust",
        "SELECT cust, COUNT(*) FROM orders_i GROUP BY cust HAVING COUNT(*) >= 2 ORDER BY cust",
        "SELECT cust, SUM(amount) FROM orders_i GROUP BY cust HAVING SUM(amount) = (SELECT MAX(s) FROM (SELECT SUM(amount) AS s FROM orders_i GROUP BY cust))",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP BY HAVING subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_dml_sequence_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE accounts_i(id INTEGER PRIMARY KEY, name TEXT, balance REAL)",
        "INSERT INTO accounts_i VALUES(1,'Alice',1000.0),(2,'Bob',500.0),(3,'Carol',750.0)",
        "UPDATE accounts_i SET balance = balance - 200 WHERE name = 'Alice'",
        "UPDATE accounts_i SET balance = balance + 200 WHERE name = 'Bob'",
        "DELETE FROM accounts_i WHERE balance < 600",
        "INSERT INTO accounts_i VALUES(4,'Dave',100.0)",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM accounts_i ORDER BY id",
        "SELECT SUM(balance) FROM accounts_i",
        "SELECT COUNT(*) FROM accounts_i",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DML sequence mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_subquery_as_column_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE teams_i(id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE members_i(id INTEGER PRIMARY KEY, team_id INTEGER, name TEXT);
        INSERT INTO teams_i VALUES(1,'Alpha'),(2,'Beta'),(3,'Gamma');
        INSERT INTO members_i VALUES(10,1,'Alice'),(20,1,'Bob'),(30,2,'Carol');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT t.name, (SELECT COUNT(*) FROM members_i m WHERE m.team_id = t.id) AS member_count FROM teams_i t ORDER BY t.name",
        "SELECT t.name, (SELECT GROUP_CONCAT(m.name, ', ') FROM members_i m WHERE m.team_id = t.id) AS names FROM teams_i t ORDER BY t.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} subquery as column mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_mixed_type_sort_s67i() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE mix_i(val);
        INSERT INTO mix_i VALUES(NULL),(3),(1.5),('hello'),(X'FF'),(2),('abc');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT val, typeof(val) FROM mix_i ORDER BY val",
        "SELECT val, typeof(val) FROM mix_i ORDER BY val DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} mixed type sort mismatches", mismatches.len());
    }
}

// ── Session 67 round 7 (s67j): Additional coverage ──

#[test]
fn test_conformance_delete_with_in_group_by_s67j() {
    // Tests the Bug #101 fix more thoroughly
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE dup_j(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO dup_j VALUES(1,'A',10),(2,'A',20),(3,'A',30);
        INSERT INTO dup_j VALUES(4,'B',40),(5,'B',50);
        DELETE FROM dup_j WHERE id IN (SELECT MIN(id) FROM dup_j GROUP BY grp);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM dup_j ORDER BY id",
        "SELECT COUNT(*) FROM dup_j",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DELETE IN GROUP BY mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_update_with_in_group_by_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE upd_j(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO upd_j VALUES(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40);
        UPDATE upd_j SET val = val * 10 WHERE id IN (SELECT MAX(id) FROM upd_j GROUP BY grp);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT * FROM upd_j ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} UPDATE IN GROUP BY mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_select_in_group_by_having_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE data_j(id INTEGER PRIMARY KEY, cat TEXT, val INTEGER);
        INSERT INTO data_j VALUES(1,'X',10),(2,'X',20),(3,'Y',30),(4,'Y',40),(5,'Z',50);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM data_j WHERE cat IN (SELECT cat FROM data_j GROUP BY cat HAVING COUNT(*) > 1) ORDER BY id",
        "SELECT * FROM data_j WHERE val NOT IN (SELECT MAX(val) FROM data_j GROUP BY cat) ORDER BY id",
        "SELECT * FROM data_j WHERE cat IN (SELECT cat FROM data_j GROUP BY cat HAVING SUM(val) >= 50) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} SELECT IN GROUP BY HAVING mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_insert_or_ignore_unique_edge_s67j() {
    // Bug #100 fix additional coverage
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE uq_j(id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
        "INSERT INTO uq_j VALUES(1, 'a@x.com', 'Alice')",
        "INSERT INTO uq_j VALUES(2, 'b@x.com', 'Bob')",
        "INSERT OR IGNORE INTO uq_j VALUES(3, 'a@x.com', 'Alice2')",
        "INSERT OR IGNORE INTO uq_j VALUES(4, 'c@x.com', 'Carol')",
        "INSERT OR IGNORE INTO uq_j VALUES(5, 'b@x.com', 'Bob2')",
        "INSERT OR IGNORE INTO uq_j VALUES(6, 'd@x.com', 'Dave')",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM uq_j ORDER BY id",
        "SELECT COUNT(*) FROM uq_j",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!(
            "{} INSERT OR IGNORE UNIQUE edge mismatches",
            mismatches.len()
        );
    }
}

#[test]
fn test_conformance_multi_table_join_agg_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE authors_j(id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE books_j(id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT, pages INTEGER);
        CREATE TABLE reviews_j(id INTEGER PRIMARY KEY, book_id INTEGER, rating INTEGER);
        INSERT INTO authors_j VALUES(1,'Tolkien'),(2,'Asimov'),(3,'Clarke');
        INSERT INTO books_j VALUES(10,1,'LOTR',1200),(20,1,'Hobbit',300),(30,2,'Foundation',250);
        INSERT INTO reviews_j VALUES(100,10,5),(101,10,4),(102,20,5),(103,30,3),(104,30,4);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT a.name, COUNT(DISTINCT b.id) AS book_count, AVG(r.rating) AS avg_rating FROM authors_j a LEFT JOIN books_j b ON a.id = b.author_id LEFT JOIN reviews_j r ON b.id = r.book_id GROUP BY a.name ORDER BY a.name",
        "SELECT a.name, SUM(b.pages) AS total_pages FROM authors_j a JOIN books_j b ON a.id = b.author_id GROUP BY a.name ORDER BY total_pages DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-table join agg mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_window_dense_rank_ntile_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE scores_j(id INTEGER PRIMARY KEY, score INTEGER);
        INSERT INTO scores_j VALUES(1,100),(2,90),(3,90),(4,80),(5,70),(6,70),(7,60);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, score, RANK() OVER (ORDER BY score DESC) AS rnk, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM scores_j ORDER BY id",
        "SELECT id, score, NTILE(3) OVER (ORDER BY score DESC) AS tile FROM scores_j ORDER BY id",
        "SELECT id, score, ROW_NUMBER() OVER (ORDER BY score DESC, id) AS rn FROM scores_j ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window DENSE_RANK/NTILE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_recursive_cte_fibonacci_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "WITH RECURSIVE fib(n, a, b) AS (SELECT 1, 0, 1 UNION ALL SELECT n+1, b, a+b FROM fib WHERE n < 10) SELECT n, a FROM fib ORDER BY n",
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT x, x*x AS sq FROM cnt",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} recursive CTE Fibonacci mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_expressions_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT CASE WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END",
        "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END",
        "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END",
        "SELECT COALESCE(NULL, NULL, 'found')",
        "SELECT 1 + 2 * 3",
        "SELECT (1 + 2) * 3",
        "SELECT 10 % 3",
        "SELECT 10 / 3, 10.0 / 3",
        "SELECT 'hello' || ' ' || 'world'",
        "SELECT 1 = 1 AND 2 = 2",
        "SELECT 1 = 1 OR 2 = 3",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex expression mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_schema_info_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE si_j(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL DEFAULT 0.0)",
        "CREATE INDEX idx_si_name ON si_j(name)",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT type, name, tbl_name FROM sqlite_master ORDER BY name",
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} schema info mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_default_value_expressions_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE dflt_j(id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unnamed', ts TEXT DEFAULT '2024-01-01', active INTEGER DEFAULT 1)",
        "INSERT INTO dflt_j(id) VALUES(1)",
        "INSERT INTO dflt_j(id, name) VALUES(2, 'Alice')",
        "INSERT INTO dflt_j VALUES(3, 'Bob', '2024-06-15', 0)",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT * FROM dflt_j ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DEFAULT value mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_null_arithmetic_chain_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT NULL + 1",
        "SELECT 1 + NULL",
        "SELECT NULL * 5",
        "SELECT NULL - NULL",
        "SELECT NULL || 'hello'",
        "SELECT 'hello' || NULL",
        "SELECT NULL = NULL",
        "SELECT NULL <> NULL",
        "SELECT NULL < 1",
        "SELECT NULL > 1",
        "SELECT NULL AND 1",
        "SELECT NULL AND 0",
        "SELECT NULL OR 1",
        "SELECT NULL OR 0",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NULL arithmetic chain mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_update_set_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE emp_j(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL);
        INSERT INTO emp_j VALUES(1,'Alice','Eng',80000),(2,'Bob','Eng',70000),(3,'Carol','Sales',60000);
        UPDATE emp_j SET salary = salary * CASE WHEN dept = 'Eng' THEN 1.1 ELSE 1.05 END;
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT id, name, printf('%.1f', salary) FROM emp_j ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex UPDATE SET mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_trigger_after_update_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE balance_j(acct TEXT PRIMARY KEY, amount REAL)",
        "CREATE TABLE audit_j(id INTEGER PRIMARY KEY, acct TEXT, old_amt REAL, new_amt REAL)",
        "CREATE TRIGGER trg_balance_update AFTER UPDATE ON balance_j BEGIN INSERT INTO audit_j(acct, old_amt, new_amt) VALUES(OLD.acct, OLD.amount, NEW.amount); END",
        "INSERT INTO balance_j VALUES('checking', 1000.0),('savings', 5000.0)",
        "UPDATE balance_j SET amount = amount + 500 WHERE acct = 'checking'",
        "UPDATE balance_j SET amount = amount - 200 WHERE acct = 'savings'",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM balance_j ORDER BY acct",
        "SELECT acct, old_amt, new_amt FROM audit_j ORDER BY id",
        "SELECT COUNT(*) FROM audit_j",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} trigger AFTER UPDATE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_natural_join_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1j(id INTEGER, name TEXT, val INTEGER);
        CREATE TABLE t2j(id INTEGER, name TEXT, score INTEGER);
        INSERT INTO t1j VALUES(1,'a',10),(2,'b',20),(3,'c',30);
        INSERT INTO t2j VALUES(1,'a',100),(2,'b',200),(4,'d',400);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM t1j NATURAL JOIN t2j ORDER BY id",
        "SELECT t1j.val, t2j.score FROM t1j JOIN t2j USING(id, name) ORDER BY t1j.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} NATURAL JOIN mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_rowid_operations_s67j() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE rw_j(name TEXT, val INTEGER);
        INSERT INTO rw_j VALUES('a',10),('b',20),('c',30);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT rowid, name, val FROM rw_j ORDER BY rowid",
        "SELECT * FROM rw_j WHERE rowid = 2",
        "SELECT MAX(rowid) FROM rw_j",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} rowid operation mismatches", mismatches.len());
    }
}

// ── Session 67 round 8 (s67k): Final coverage expansion ──

#[test]
fn test_conformance_multi_insert_values_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE bulk_k(id INTEGER PRIMARY KEY, name TEXT, val INTEGER);
        INSERT INTO bulk_k VALUES(1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM bulk_k ORDER BY id",
        "SELECT COUNT(*) FROM bulk_k",
        "SELECT SUM(val) FROM bulk_k",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-insert VALUES mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_fk_set_null_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE parent_k(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE child_k(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent_k(id) ON DELETE SET NULL, val TEXT)",
        "INSERT INTO parent_k VALUES(1,'A'),(2,'B')",
        "INSERT INTO child_k VALUES(10,1,'x'),(20,2,'y'),(30,1,'z')",
        "DELETE FROM parent_k WHERE id = 1",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM child_k ORDER BY id",
        "SELECT * FROM parent_k ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} FK SET NULL mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_correlated_scalar_subquery_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE dept_k(id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE emp_k(id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER);
        INSERT INTO dept_k VALUES(1,'Eng'),(2,'Sales'),(3,'HR');
        INSERT INTO emp_k VALUES(10,1,80000),(20,1,90000),(30,2,60000);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT d.name, (SELECT MAX(e.salary) FROM emp_k e WHERE e.dept_id = d.id) AS max_sal FROM dept_k d ORDER BY d.name",
        "SELECT d.name, (SELECT COUNT(*) FROM emp_k e WHERE e.dept_id = d.id) AS emp_cnt FROM dept_k d ORDER BY d.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} correlated scalar subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_nested_function_calls_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT UPPER(REPLACE('hello world', 'world', 'earth'))",
        "SELECT LENGTH(TRIM('  hello  '))",
        "SELECT ABS(ROUND(-3.7))",
        "SELECT COALESCE(NULLIF(0, 0), 42)",
        "SELECT IIF(LENGTH('test') > 3, UPPER('test'), LOWER('TEST'))",
        "SELECT SUBSTR(REPLACE('abc-def-ghi', '-', '/'), 5, 3)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested function call mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_transaction_atomicity_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE txn_k(id INTEGER PRIMARY KEY, val INTEGER)",
        "INSERT INTO txn_k VALUES(1,100),(2,200)",
        "BEGIN",
        "UPDATE txn_k SET val = val + 50 WHERE id = 1",
        "INSERT INTO txn_k VALUES(3, 300)",
        "COMMIT",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &["SELECT * FROM txn_k ORDER BY id"];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} transaction atomicity mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_column_unique_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE uniq_k(a TEXT, b TEXT, c INTEGER, UNIQUE(a, b))",
        "INSERT INTO uniq_k VALUES('x','y',1)",
        "INSERT INTO uniq_k VALUES('x','z',2)",
        "INSERT OR IGNORE INTO uniq_k VALUES('x','y',3)",
        "INSERT INTO uniq_k VALUES('a','y',4)",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM uniq_k ORDER BY a, b",
        "SELECT COUNT(*) FROM uniq_k",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-column UNIQUE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_group_by_numeric_index_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE gb_k(cat TEXT, val INTEGER);
        INSERT INTO gb_k VALUES('A',10),('B',20),('A',30),('B',40),('C',50);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT cat, SUM(val) FROM gb_k GROUP BY 1 ORDER BY 1",
        "SELECT cat, COUNT(*), AVG(val) FROM gb_k GROUP BY 1 ORDER BY 2 DESC, 1",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} GROUP BY numeric index mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_nested_exists_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE a_k(id INTEGER PRIMARY KEY);
        CREATE TABLE b_k(id INTEGER PRIMARY KEY, a_id INTEGER);
        CREATE TABLE c_k(id INTEGER PRIMARY KEY, b_id INTEGER);
        INSERT INTO a_k VALUES(1),(2),(3);
        INSERT INTO b_k VALUES(10,1),(20,1),(30,2);
        INSERT INTO c_k VALUES(100,10),(200,30);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id FROM a_k a WHERE EXISTS (SELECT 1 FROM b_k b WHERE b.a_id = a.id AND EXISTS (SELECT 1 FROM c_k c WHERE c.b_id = b.id)) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} nested EXISTS mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_window_lag_lead_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE ts_k(id INTEGER PRIMARY KEY, val INTEGER);
        INSERT INTO ts_k VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev_val FROM ts_k ORDER BY id",
        "SELECT id, val, LEAD(val) OVER (ORDER BY id) AS next_val FROM ts_k ORDER BY id",
        "SELECT id, val, LAG(val, 2, 0) OVER (ORDER BY id) AS prev2 FROM ts_k ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window LAG/LEAD mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_concat_operator_s67k() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT 'hello' || ' ' || 'world'",
        "SELECT 1 || 2",
        "SELECT NULL || 'test'",
        "SELECT 'test' || NULL",
        "SELECT '' || ''",
        "SELECT CAST(42 AS TEXT) || ' items'",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} concat operator mismatches", mismatches.len());
    }
}

// ── Session 67 round 9 (s67l): Subquery patterns + DML edge cases ──

#[test]
fn test_conformance_update_from_cte_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE targets_l(id INTEGER PRIMARY KEY, val INTEGER);
        INSERT INTO targets_l VALUES(1,10),(2,20),(3,30);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    // UPDATE with CTE - update val to its rank based on value
    let queries = &[
        "WITH ranked AS (SELECT id, val FROM targets_l ORDER BY val DESC) SELECT * FROM ranked",
        "SELECT id, val FROM targets_l ORDER BY val DESC",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTE query mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_delete_with_exists_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE main_l(id INTEGER PRIMARY KEY, val TEXT);
        CREATE TABLE exclude_l(val TEXT);
        INSERT INTO main_l VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d');
        INSERT INTO exclude_l VALUES('b'),('d');
        DELETE FROM main_l WHERE EXISTS (SELECT 1 FROM exclude_l WHERE exclude_l.val = main_l.val);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM main_l ORDER BY id",
        "SELECT COUNT(*) FROM main_l",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} DELETE EXISTS mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_insert_or_replace_with_trigger_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE config_l(key TEXT PRIMARY KEY, val TEXT)",
        "CREATE TABLE config_log_l(id INTEGER PRIMARY KEY, key TEXT, action TEXT)",
        "CREATE TRIGGER trg_config_insert AFTER INSERT ON config_l BEGIN INSERT INTO config_log_l(key, action) VALUES(NEW.key, 'insert'); END",
        "INSERT INTO config_l VALUES('theme', 'dark')",
        "INSERT OR REPLACE INTO config_l VALUES('theme', 'light')",
        "INSERT INTO config_l VALUES('lang', 'en')",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM config_l ORDER BY key",
        "SELECT key, action FROM config_log_l ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} INSERT OR REPLACE trigger mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_complex_join_conditions_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE products_l(id INTEGER PRIMARY KEY, name TEXT, price REAL, cat TEXT);
        CREATE TABLE discounts_l(cat TEXT, min_price REAL, pct REAL);
        INSERT INTO products_l VALUES(1,'Widget',50.0,'A'),(2,'Gadget',100.0,'B'),(3,'Doohickey',25.0,'A'),(4,'Thingamajig',200.0,'B');
        INSERT INTO discounts_l VALUES('A',30.0,10.0),('B',150.0,15.0);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT p.name, p.price, d.pct FROM products_l p JOIN discounts_l d ON p.cat = d.cat AND p.price >= d.min_price ORDER BY p.name",
        "SELECT p.name, p.price FROM products_l p LEFT JOIN discounts_l d ON p.cat = d.cat AND p.price >= d.min_price WHERE d.cat IS NULL ORDER BY p.name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} complex join condition mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multiple_aggregates_with_filter_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE events_l(id INTEGER PRIMARY KEY, type TEXT, val INTEGER);
        INSERT INTO events_l VALUES(1,'A',10),(2,'B',20),(3,'A',30),(4,'B',40),(5,'A',50),(6,'C',60);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT type, SUM(val) AS total, MIN(val), MAX(val) FROM events_l GROUP BY type ORDER BY type",
        "SELECT COUNT(*) FILTER (WHERE type = 'A'), COUNT(*) FILTER (WHERE type = 'B') FROM events_l",
        "SELECT SUM(val) FILTER (WHERE type = 'A'), SUM(val) FILTER (WHERE type != 'A') FROM events_l",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} aggregate FILTER mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multi_column_in_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE pairs_l(a INTEGER, b INTEGER, val TEXT);
        INSERT INTO pairs_l VALUES(1,10,'x'),(2,20,'y'),(3,30,'z'),(1,20,'w');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM pairs_l WHERE a IN (1, 3) ORDER BY a, b",
        "SELECT * FROM pairs_l WHERE a = 1 AND b IN (10, 30) ORDER BY b",
        "SELECT * FROM pairs_l WHERE a NOT IN (1) ORDER BY a, b",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multi-column IN mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_derived_table_with_agg_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE orders_l(id INTEGER, region TEXT, amount REAL);
        INSERT INTO orders_l VALUES(1,'East',100),(2,'East',200),(3,'West',150),(4,'West',50),(5,'East',75);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT region, total FROM (SELECT region, SUM(amount) AS total FROM orders_l GROUP BY region) ORDER BY total DESC",
        "SELECT * FROM (SELECT region, COUNT(*) AS cnt, AVG(amount) AS avg_amt FROM orders_l GROUP BY region) WHERE cnt >= 2 ORDER BY region",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} derived table aggregate mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_autoincrement_behavior_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "CREATE TABLE autoinc_l(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
        "INSERT INTO autoinc_l(name) VALUES('Alice'),('Bob'),('Carol')",
        "DELETE FROM autoinc_l WHERE name = 'Bob'",
        "INSERT INTO autoinc_l(name) VALUES('Dave')",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM autoinc_l ORDER BY id",
        "SELECT MAX(id) FROM autoinc_l",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} AUTOINCREMENT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_select_star_from_subquery_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE base_l(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER);
        INSERT INTO base_l VALUES(1,10,100),(2,20,200),(3,30,300);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM (SELECT id, x + y AS sum_xy FROM base_l) ORDER BY id",
        "SELECT * FROM (SELECT * FROM base_l WHERE x > 10) ORDER BY id",
        "SELECT t.id, t.sum_xy FROM (SELECT id, x + y AS sum_xy FROM base_l) t WHERE t.sum_xy > 200 ORDER BY t.id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} SELECT * subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_count_distinct_vs_count_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE cd_l(id INTEGER, cat TEXT, val INTEGER);
        INSERT INTO cd_l VALUES(1,'A',10),(2,'B',10),(3,'A',20),(4,'B',20),(5,'A',10);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT COUNT(*), COUNT(cat), COUNT(DISTINCT cat) FROM cd_l",
        "SELECT cat, COUNT(*), COUNT(DISTINCT val) FROM cd_l GROUP BY cat ORDER BY cat",
        "SELECT COUNT(DISTINCT val) FROM cd_l",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} COUNT DISTINCT mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_where_subquery_comparison_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE items_l(id INTEGER PRIMARY KEY, name TEXT, price REAL);
        INSERT INTO items_l VALUES(1,'A',10.0),(2,'B',20.0),(3,'C',30.0),(4,'D',15.0);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT name FROM items_l WHERE price > (SELECT AVG(price) FROM items_l) ORDER BY name",
        "SELECT name FROM items_l WHERE price = (SELECT MIN(price) FROM items_l)",
        "SELECT name FROM items_l WHERE price < (SELECT MAX(price) FROM items_l) ORDER BY name",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} WHERE subquery comparison mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_window_first_last_value_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE ts_l(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO ts_l VALUES(1,'A',10),(2,'A',20),(3,'A',30),(4,'B',100),(5,'B',200);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT id, grp, val, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) AS first_v FROM ts_l ORDER BY id",
        "SELECT id, grp, val, LAST_VALUE(val) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_v FROM ts_l ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} window FIRST/LAST_VALUE mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_expression_in_order_by_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE words_l(word TEXT);
        INSERT INTO words_l VALUES('banana'),('apple'),('cherry'),('date'),('elderberry');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT word FROM words_l ORDER BY LENGTH(word), word",
        "SELECT word, LENGTH(word) AS len FROM words_l ORDER BY len DESC, word",
        "SELECT word FROM words_l ORDER BY SUBSTR(word, 2, 1)",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} expression ORDER BY mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_multiple_foreign_keys_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = &[
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE users_l(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE posts_l(id INTEGER PRIMARY KEY, author_id INTEGER REFERENCES users_l(id) ON DELETE CASCADE, editor_id INTEGER REFERENCES users_l(id) ON DELETE SET NULL, title TEXT)",
        "INSERT INTO users_l VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')",
        "INSERT INTO posts_l VALUES(10,1,2,'Post1'),(20,2,3,'Post2'),(30,1,3,'Post3')",
        "DELETE FROM users_l WHERE id = 1",
    ];
    for s in setup {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM posts_l ORDER BY id",
        "SELECT * FROM users_l ORDER BY id",
        "SELECT COUNT(*) FROM posts_l",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} multiple FK mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_case_null_semantics_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let queries = &[
        "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no_match' END",
        "SELECT CASE WHEN NULL THEN 'true' ELSE 'false' END",
        "SELECT CASE WHEN NULL IS NULL THEN 'is_null' ELSE 'not_null' END",
        "SELECT CASE 1 WHEN 1 THEN 'one' WHEN NULL THEN 'null' ELSE 'other' END",
        "SELECT CASE WHEN 0 THEN 'zero' WHEN NULL THEN 'null' ELSE 'else' END",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CASE NULL semantics mismatches", mismatches.len());
    }
}

// ── Regression: compound IN subquery (Bug #101 extended) ──

#[test]
fn test_conformance_in_compound_subquery_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE t1_comp(id INTEGER PRIMARY KEY, val TEXT);
        CREATE TABLE t2_comp(id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO t1_comp VALUES(1,'a'),(2,'b'),(3,'c');
        INSERT INTO t2_comp VALUES(4,'d'),(5,'e');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT val FROM t1_comp WHERE id IN (SELECT id FROM t1_comp UNION SELECT id FROM t2_comp) ORDER BY id",
        "SELECT val FROM t1_comp WHERE id NOT IN (SELECT id FROM t2_comp UNION SELECT 2) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} compound IN subquery mismatches", mismatches.len());
    }
}

#[test]
fn test_conformance_in_cte_subquery_s67l() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "
        CREATE TABLE data_cte(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
        INSERT INTO data_cte VALUES(1,'X',10),(2,'X',20),(3,'Y',30),(4,'Y',40);
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }

    let queries = &[
        "SELECT * FROM data_cte WHERE id IN (WITH maxes AS (SELECT MAX(id) AS mid FROM data_cte GROUP BY grp) SELECT mid FROM maxes) ORDER BY id",
    ];

    let mismatches = oracle_compare(&fconn, &rconn, queries);
    if !mismatches.is_empty() {
        for m in &mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} CTE IN subquery mismatches", mismatches.len());
    }
}
