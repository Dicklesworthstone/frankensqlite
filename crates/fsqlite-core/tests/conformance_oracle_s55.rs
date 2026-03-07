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

    for s in &["CREATE TABLE eta(id INTEGER PRIMARY KEY, val INTEGER, name TEXT)"] {
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
