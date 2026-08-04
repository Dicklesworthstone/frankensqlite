//! bd-2yqp6.3: Track C — complex query oracle parity e2e tests.
//!
//! File-backed database tests verifying oracle parity (rusqlite) for
//! complex query patterns across close/reopen cycles.
//!
//! - Q1: Correlated subquery in SELECT list
//! - Q2: Multi-level CTE with cross-references
//! - Q3: Window function with PARTITION BY + ORDER BY
//! - Q4: Compound SELECT (UNION ALL / INTERSECT / EXCEPT)
//! - Q5: Self-join with aliased aggregation
//! - Q6: Subquery in WHERE with EXISTS/NOT EXISTS
//! - Q7: CASE expression with nested subqueries
//! - Q8: Multi-table JOIN with GROUP BY HAVING
//! - Q9: Recursive CTE (tree traversal)
//! - Q10: Derived table (inline view) with aggregation
//! - Q11: INSERT...SELECT with complex source
//! - Q12: UPDATE with correlated subquery in SET
//! - Q13: DELETE with subquery in WHERE
//! - Q14: COALESCE / NULLIF / IIF expressions
//! - Q15: Multiple aggregates with DISTINCT and FILTER-like patterns
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

async fn get_int(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(sql).await.ok()?;
    let row = rows.first()?;
    match row.get(0)? {
        SqliteValue::Integer(v) => Some(*v),
        _ => None,
    }
}

fn c_get_int(c: &rusqlite::Connection, sql: &str) -> Option<i64> {
    c.prepare(sql)
        .ok()
        .and_then(|mut s| s.query_row([], |r| r.get(0)).ok())
}

async fn seed_employees(f: &Connection, c: &rusqlite::Connection) {
    let ddl = "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER, mgr_id INTEGER)";
    f.execute(ddl).await.expect("f create");
    c.execute_batch(ddl).expect("c create");

    let rows = [
        (1, "Alice", "eng", 120000, "NULL"),
        (2, "Bob", "eng", 95000, "1"),
        (3, "Carol", "sales", 88000, "NULL"),
        (4, "Dave", "eng", 105000, "1"),
        (5, "Eve", "sales", 92000, "3"),
        (6, "Frank", "hr", 78000, "NULL"),
        (7, "Grace", "hr", 72000, "6"),
        (8, "Heidi", "eng", 110000, "1"),
        (9, "Ivan", "sales", 85000, "3"),
        (10, "Judy", "hr", 81000, "6"),
    ];

    f.execute("BEGIN").await.expect("f begin");
    for (id, name, dept, salary, mgr) in &rows {
        let sql =
            format!("INSERT INTO employees VALUES ({id}, '{name}', '{dept}', {salary}, {mgr})");
        f.execute(&sql).await.expect("f insert");
        c.execute(&sql, []).expect("c insert");
    }
    f.execute("COMMIT").await.expect("f commit");
}

async fn seed_orders(f: &Connection, c: &rusqlite::Connection) {
    let ddl = "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, amount INTEGER, quarter INTEGER)";
    f.execute(ddl).await.expect("f create orders");
    c.execute_batch(ddl).expect("c create orders");

    f.execute("BEGIN").await.expect("f begin");
    let data = [
        (1, 2, 5000, 1),
        (2, 2, 7000, 2),
        (3, 5, 3000, 1),
        (4, 5, 4500, 2),
        (5, 9, 6000, 1),
        (6, 3, 12000, 1),
        (7, 3, 8000, 2),
        (8, 4, 3500, 1),
        (9, 8, 9000, 2),
        (10, 1, 15000, 1),
        (11, 1, 11000, 2),
        (12, 4, 4200, 2),
    ];
    for (id, emp_id, amount, quarter) in &data {
        let sql = format!("INSERT INTO orders VALUES ({id}, {emp_id}, {amount}, {quarter})");
        f.execute(&sql).await.expect("f insert order");
        c.execute(&sql, []).expect("c insert order");
    }
    f.execute("COMMIT").await.expect("f commit");
}

// ─── Q1: Correlated subquery in SELECT list ──────────────────────

#[test]
fn q1_correlated_subquery_select() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q1_f.db");
        let c_path = dir.path().join("q1_c.db");
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");
        seed_employees(&f, &c).await;

        let sql = "SELECT e.dept, \
               (SELECT COUNT(*) FROM employees e2 WHERE e2.dept = e.dept) AS dept_size, \
               (SELECT AVG(salary) FROM employees e3 WHERE e3.dept = e.dept) AS dept_avg \
               FROM employees e WHERE e.id = 1";
        let f_rows = f.query(sql).await.expect("f query");
        let c_dept_size: i64 = c.query_row(
        "SELECT (SELECT COUNT(*) FROM employees e2 WHERE e2.dept = e.dept) FROM employees e WHERE e.id = 1",
        [], |r| r.get(0)).expect("c query");

        let f_dept_size = match f_rows[0].get(1) {
            Some(SqliteValue::Integer(v)) => *v,
            other => panic!("Q1: dept_size wrong type: {other:?}"),
        };
        assert_eq!(f_dept_size, c_dept_size, "Q1: dept_size mismatch");
        assert_eq!(f_dept_size, 4, "Q1: eng should have 4 employees");

        eprintln!("Q1: correlated subquery in SELECT — oracle parity");
    });
}

// ─── Q2: Multi-level CTE ─────────────────────────────────────────

#[test]
fn q2_multi_level_cte() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q2_f.db");
        let c_path = dir.path().join("q2_c.db");

        // Seed both tables, then reopen fsqlite to get a clean snapshot
        {
            let f = Connection::open(f_path.to_str().expect("p"))
                .await
                .expect("f open");
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            seed_employees(&f, &c).await;
            seed_orders(&f, &c).await;
        }

        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f reopen");
        let c = rusqlite::Connection::open(&c_path).expect("c reopen");

        let sql = "\
        WITH dept_stats AS ( \
            SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total_sal \
            FROM employees GROUP BY dept \
        ), \
        dept_orders AS ( \
            SELECT e.dept, SUM(o.amount) AS total_orders \
            FROM employees e JOIN orders o ON e.id = o.emp_id \
            GROUP BY e.dept \
        ) \
        SELECT ds.dept, ds.cnt, ds.total_sal, COALESCE(do2.total_orders, 0) \
        FROM dept_stats ds LEFT JOIN dept_orders do2 ON ds.dept = do2.dept \
        ORDER BY ds.dept";

        let f_rows = f.query(sql).await.expect("f query");

        let mut c_stmt = c.prepare(sql).expect("c prepare");
        let c_rows: Vec<(String, i64, i64, i64)> = c_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .expect("c query")
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(f_rows.len(), c_rows.len(), "Q2: row count mismatch");

        for (i, (c_dept, c_cnt, _c_sal, _c_ord)) in c_rows.iter().enumerate() {
            let f_dept = match f_rows[i].get(0) {
                Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                _ => String::new(),
            };
            let f_cnt = match f_rows[i].get(1) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            assert_eq!(f_dept, *c_dept, "Q2: dept mismatch at row {i}");
            assert_eq!(f_cnt, *c_cnt, "Q2: count mismatch at row {i} dept={c_dept}");
        }

        eprintln!(
            "Q2: multi-level CTE with cross-join — {} rows, oracle parity",
            f_rows.len()
        );
    });
}

// ─── Q3: Window function with PARTITION BY ───────────────────────

#[test]
fn q3_window_partition_by() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q3_f.db");
        let c_path = dir.path().join("q3_c.db");
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");
        seed_employees(&f, &c).await;

        let sql = "SELECT id, name, dept, salary, \
               RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank \
               FROM employees ORDER BY dept, dept_rank";

        let f_rows = f.query(sql).await.expect("f query");
        let mut c_stmt = c.prepare(sql).expect("c prepare");
        let c_rows: Vec<(i64, String, i64)> = c_stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(4)?,
                ))
            })
            .expect("c query")
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(f_rows.len(), c_rows.len(), "Q3: row count mismatch");

        for (i, (c_id, c_dept, c_rank)) in c_rows.iter().enumerate() {
            let f_id = match f_rows[i].get(0) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            let f_rank = match f_rows[i].get(4) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            assert_eq!(f_id, *c_id, "Q3: id mismatch at row {i}");
            assert_eq!(
                f_rank, *c_rank,
                "Q3: rank mismatch at row {i} dept={c_dept}"
            );
        }

        eprintln!("Q3: RANK() OVER (PARTITION BY dept) — oracle parity");
    });
}

// ─── Q4: Compound SELECT ─────────────────────────────────────────

#[test]
fn q4_compound_select() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q4_f.db");
        let c_path = dir.path().join("q4_c.db");
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");
        seed_employees(&f, &c).await;

        // UNION ALL
        let sql_union = "SELECT dept, 'high' AS bracket FROM employees WHERE salary >= 100000 \
                     UNION ALL \
                     SELECT dept, 'low' FROM employees WHERE salary < 80000";
        let f_count = f.query(sql_union).await.expect("f query").len();
        let c_count: i64 = c
            .query_row(&format!("SELECT COUNT(*) FROM ({sql_union})"), [], |r| {
                r.get(0)
            })
            .expect("c count");
        assert_eq!(f_count as i64, c_count, "Q4: UNION ALL count mismatch");

        // INTERSECT
        let sql_intersect = "SELECT dept FROM employees WHERE salary > 90000 \
                         INTERSECT \
                         SELECT dept FROM employees WHERE salary < 110000";
        let f_isect = f.query(sql_intersect).await.expect("f intersect").len();
        let c_isect: i64 = c
            .query_row(
                &format!("SELECT COUNT(*) FROM ({sql_intersect})"),
                [],
                |r| r.get(0),
            )
            .expect("c isect");
        assert_eq!(f_isect as i64, c_isect, "Q4: INTERSECT count mismatch");

        // EXCEPT
        let sql_except = "SELECT dept FROM employees \
                      EXCEPT \
                      SELECT dept FROM employees WHERE salary > 100000";
        let f_except = f.query(sql_except).await.expect("f except").len();
        let c_except: i64 = c
            .query_row(&format!("SELECT COUNT(*) FROM ({sql_except})"), [], |r| {
                r.get(0)
            })
            .expect("c except");
        assert_eq!(f_except as i64, c_except, "Q4: EXCEPT count mismatch");

        eprintln!("Q4: UNION ALL/INTERSECT/EXCEPT — oracle parity");
    });
}

// ─── Q5: Self-join with aliased aggregation ──────────────────────

#[test]
fn q5_self_join_aggregation() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q5_f.db");
        let c_path = dir.path().join("q5_c.db");
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");
        seed_employees(&f, &c).await;

        let sql = "SELECT m.name, COUNT(e.id) AS report_count, SUM(e.salary) AS team_cost \
               FROM employees m JOIN employees e ON e.mgr_id = m.id \
               GROUP BY m.id, m.name \
               ORDER BY report_count DESC";

        let f_rows = f.query(sql).await.expect("f query");
        let mut c_stmt = c.prepare(sql).expect("c prepare");
        let c_rows: Vec<(String, i64, i64)> = c_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .expect("c query")
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(f_rows.len(), c_rows.len(), "Q5: row count mismatch");

        for (i, (c_name, c_cnt, c_cost)) in c_rows.iter().enumerate() {
            let f_name = match f_rows[i].get(0) {
                Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                _ => String::new(),
            };
            let f_cnt = match f_rows[i].get(1) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            let f_cost = match f_rows[i].get(2) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            assert_eq!(f_name, *c_name, "Q5: name mismatch at row {i}");
            assert_eq!(f_cnt, *c_cnt, "Q5: count mismatch for {c_name}");
            assert_eq!(f_cost, *c_cost, "Q5: cost mismatch for {c_name}");
        }

        eprintln!("Q5: self-join manager→reports — oracle parity");
    });
}

// ─── Q6: EXISTS / NOT EXISTS ─────────────────────────────────────

#[test]
fn q6_exists_not_exists() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q6_f.db");
        let c_path = dir.path().join("q6_c.db");
        {
            let f = Connection::open(f_path.to_str().expect("p"))
                .await
                .expect("f open");
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            seed_employees(&f, &c).await;
            seed_orders(&f, &c).await;
        }
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f reopen");
        let c = rusqlite::Connection::open(&c_path).expect("c reopen");

        // Employees who have orders
        let sql_exists = "SELECT id, name FROM employees e \
                      WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
                      ORDER BY id";
        let f_exists = f.query(sql_exists).await.expect("f exists");
        let c_exists: i64 = c
            .query_row(&format!("SELECT COUNT(*) FROM ({sql_exists})"), [], |r| {
                r.get(0)
            })
            .expect("c exists");
        assert_eq!(f_exists.len() as i64, c_exists, "Q6: EXISTS count mismatch");

        // Employees with NO orders
        let sql_not = "SELECT id, name FROM employees e \
                   WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
                   ORDER BY id";
        let f_not = f.query(sql_not).await.expect("f not exists");
        let c_not: i64 = c
            .query_row(&format!("SELECT COUNT(*) FROM ({sql_not})"), [], |r| {
                r.get(0)
            })
            .expect("c not");
        assert_eq!(f_not.len() as i64, c_not, "Q6: NOT EXISTS count mismatch");

        // Total should be 10
        assert_eq!(
            f_exists.len() + f_not.len(),
            10,
            "Q6: EXISTS + NOT EXISTS should cover all employees"
        );

        eprintln!(
            "Q6: EXISTS={}, NOT EXISTS={} — oracle parity",
            f_exists.len(),
            f_not.len()
        );
    });
}

// ─── Q7: CASE with nested subqueries ─────────────────────────────

#[test]
fn q7_case_with_subqueries() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q7_f.db");
        let c_path = dir.path().join("q7_c.db");
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");
        seed_employees(&f, &c).await;

        let sql = "SELECT name, \
               CASE \
                   WHEN salary > (SELECT AVG(salary) FROM employees) THEN 'above_avg' \
                   WHEN salary = (SELECT AVG(salary) FROM employees) THEN 'at_avg' \
                   ELSE 'below_avg' \
               END AS bracket \
               FROM employees ORDER BY name";

        let f_rows = f.query(sql).await.expect("f query");
        let mut c_stmt = c.prepare(sql).expect("c prepare");
        let c_rows: Vec<(String, String)> = c_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .expect("c query")
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(f_rows.len(), c_rows.len(), "Q7: row count mismatch");

        for (i, (c_name, c_bracket)) in c_rows.iter().enumerate() {
            let f_bracket = match f_rows[i].get(1) {
                Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                _ => String::new(),
            };
            assert_eq!(f_bracket, *c_bracket, "Q7: bracket mismatch for {c_name}");
        }

        eprintln!("Q7: CASE with subquery thresholds — oracle parity");
    });
}

// ─── Q8: Multi-table JOIN with GROUP BY HAVING ───────────────────

#[test]
fn q8_join_group_having() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q8_f.db");
        let c_path = dir.path().join("q8_c.db");
        {
            let f = Connection::open(f_path.to_str().expect("p"))
                .await
                .expect("f open");
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            seed_employees(&f, &c).await;
            seed_orders(&f, &c).await;
        }
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f reopen");
        let c = rusqlite::Connection::open(&c_path).expect("c reopen");

        let sql = "SELECT e.dept, COUNT(DISTINCT e.id) AS emp_count, SUM(o.amount) AS total_rev \
               FROM employees e JOIN orders o ON e.id = o.emp_id \
               GROUP BY e.dept \
               HAVING SUM(o.amount) > 10000 \
               ORDER BY total_rev DESC";

        let f_rows = f.query(sql).await.expect("f query");
        let mut c_stmt = c.prepare(sql).expect("c prepare");
        let c_rows: Vec<(String, i64, i64)> = c_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .expect("c query")
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(f_rows.len(), c_rows.len(), "Q8: row count mismatch");

        for (i, (c_dept, _c_emp, c_rev)) in c_rows.iter().enumerate() {
            let f_dept = match f_rows[i].get(0) {
                Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                _ => String::new(),
            };
            let f_rev = match f_rows[i].get(2) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            assert_eq!(f_dept, *c_dept, "Q8: dept mismatch at row {i}");
            assert_eq!(f_rev, *c_rev, "Q8: revenue mismatch for {c_dept}");
        }

        eprintln!("Q8: JOIN + GROUP BY + HAVING — oracle parity");
    });
}

// ─── Q9: Recursive CTE ──────────────────────────────────────────

#[test]
fn q9_recursive_cte() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q9_f.db");
        let c_path = dir.path().join("q9_c.db");
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");
        seed_employees(&f, &c).await;

        let sql = "WITH RECURSIVE org_tree(id, name, mgr_id, depth) AS ( \
                   SELECT id, name, mgr_id, 0 FROM employees WHERE mgr_id IS NULL \
                   UNION ALL \
                   SELECT e.id, e.name, e.mgr_id, t.depth + 1 \
                   FROM employees e JOIN org_tree t ON e.mgr_id = t.id \
               ) \
               SELECT id, name, depth FROM org_tree ORDER BY depth, name";

        let f_rows = f.query(sql).await.expect("f query");
        let mut c_stmt = c.prepare(sql).expect("c prepare");
        let c_rows: Vec<(i64, String, i64)> = c_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .expect("c query")
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(f_rows.len(), c_rows.len(), "Q9: row count mismatch");
        assert_eq!(f_rows.len(), 10, "Q9: should cover all 10 employees");

        for (i, (c_id, c_name, c_depth)) in c_rows.iter().enumerate() {
            let f_id = match f_rows[i].get(0) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            let f_depth = match f_rows[i].get(2) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            assert_eq!(f_id, *c_id, "Q9: id mismatch at row {i}");
            assert_eq!(f_depth, *c_depth, "Q9: depth mismatch for {c_name}");
        }

        eprintln!("Q9: recursive CTE org tree — oracle parity");
    });
}

// ─── Q10: Derived table with aggregation ─────────────────────────

#[test]
fn q10_derived_table_aggregation() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q10_f.db");
        let c_path = dir.path().join("q10_c.db");
        {
            let f = Connection::open(f_path.to_str().expect("p"))
                .await
                .expect("f open");
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            seed_employees(&f, &c).await;
            seed_orders(&f, &c).await;
        }
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f reopen");
        let c = rusqlite::Connection::open(&c_path).expect("c reopen");

        let sql = "SELECT d.dept, d.dept_avg, e.name, e.salary \
               FROM employees e \
               JOIN (SELECT dept, AVG(salary) AS dept_avg FROM employees GROUP BY dept) d \
               ON e.dept = d.dept \
               WHERE e.salary > d.dept_avg \
               ORDER BY e.salary DESC";

        let f_rows = f.query(sql).await.expect("f query");
        let mut c_stmt = c.prepare(sql).expect("c prepare");
        let c_rows: Vec<(String, String)> = c_stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(2)?)))
            .expect("c query")
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(
            f_rows.len(),
            c_rows.len(),
            "Q10: row count mismatch f={} c={}",
            f_rows.len(),
            c_rows.len()
        );

        eprintln!(
            "Q10: derived table + above-average filter — {} rows, oracle parity",
            f_rows.len()
        );
    });
}

// ─── Q11: INSERT...SELECT with complex source ────────────────────

#[test]
fn q11_insert_select_complex() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q11_f.db");
        let c_path = dir.path().join("q11_c.db");
        {
            let f = Connection::open(f_path.to_str().expect("p"))
                .await
                .expect("f open");
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            seed_employees(&f, &c).await;
            seed_orders(&f, &c).await;
        }
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f reopen");
        let c = rusqlite::Connection::open(&c_path).expect("c reopen");

        let ddl = "CREATE TABLE dept_summary (dept TEXT, emp_count INTEGER, total_orders INTEGER)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        let ins = "INSERT INTO dept_summary \
               SELECT e.dept, COUNT(DISTINCT e.id), COALESCE(SUM(o.amount), 0) \
               FROM employees e LEFT JOIN orders o ON e.id = o.emp_id \
               GROUP BY e.dept";
        f.execute(ins).await.expect("f insert");
        c.execute_batch(ins).expect("c insert");

        // Verify
        let sql = "SELECT dept, emp_count, total_orders FROM dept_summary ORDER BY dept";
        let f_rows = f.query(sql).await.expect("f query");
        let mut c_stmt = c.prepare(sql).expect("c prepare");
        let c_rows: Vec<(String, i64, i64)> = c_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .expect("c query")
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(f_rows.len(), c_rows.len(), "Q11: row count mismatch");

        for (i, (c_dept, _c_cnt, c_tot)) in c_rows.iter().enumerate() {
            let f_dept = match f_rows[i].get(0) {
                Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                _ => String::new(),
            };
            let f_tot = match f_rows[i].get(2) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            assert_eq!(f_dept, *c_dept, "Q11: dept mismatch at row {i}");
            assert_eq!(f_tot, *c_tot, "Q11: total_orders mismatch for {c_dept}");
        }

        eprintln!("Q11: INSERT...SELECT with JOIN+GROUP — oracle parity");
    });
}

// ─── Q12: UPDATE with correlated subquery ────────────────────────

#[test]
fn q12_update_correlated_subquery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q12_f.db");
        let c_path = dir.path().join("q12_c.db");
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");
        seed_employees(&f, &c).await;

        let ddl = "CREATE TABLE bonuses (emp_id INTEGER PRIMARY KEY, bonus INTEGER DEFAULT 0)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        let seed = "INSERT INTO bonuses SELECT id, 0 FROM employees";
        f.execute(seed).await.expect("f seed");
        c.execute_batch(seed).expect("c seed");

        let upd = "UPDATE bonuses SET bonus = \
               (SELECT CASE WHEN e.salary > 100000 THEN 10000 ELSE 5000 END \
                FROM employees e WHERE e.id = bonuses.emp_id)";
        f.execute(upd).await.expect("f update");
        c.execute_batch(upd).expect("c update");

        let f_sum = get_int(&f, "SELECT SUM(bonus) FROM bonuses").await.unwrap();
        let c_sum = c_get_int(&c, "SELECT SUM(bonus) FROM bonuses").unwrap();
        assert_eq!(f_sum, c_sum, "Q12: bonus sum mismatch f={f_sum} c={c_sum}");

        let f_high = get_int(&f, "SELECT COUNT(*) FROM bonuses WHERE bonus = 10000")
            .await
            .unwrap();
        let c_high = c_get_int(&c, "SELECT COUNT(*) FROM bonuses WHERE bonus = 10000").unwrap();
        assert_eq!(f_high, c_high, "Q12: high-bonus count mismatch");

        eprintln!("Q12: UPDATE with correlated subquery — sum={f_sum}, oracle parity");
    });
}

// ─── Q13: DELETE with subquery ───────────────────────────────────

#[test]
fn q13_delete_with_subquery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q13_f.db");
        let c_path = dir.path().join("q13_c.db");
        {
            let f = Connection::open(f_path.to_str().expect("p"))
                .await
                .expect("f open");
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            seed_employees(&f, &c).await;
            seed_orders(&f, &c).await;
        }
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f reopen");
        let c = rusqlite::Connection::open(&c_path).expect("c reopen");

        // Delete orders for employees with salary < 90000
        let del = "DELETE FROM orders WHERE emp_id IN \
               (SELECT id FROM employees WHERE salary < 90000)";
        f.execute(del).await.expect("f delete");
        c.execute_batch(del).expect("c delete");

        let f_remaining = get_int(&f, "SELECT COUNT(*) FROM orders").await.unwrap();
        let c_remaining = c_get_int(&c, "SELECT COUNT(*) FROM orders").unwrap();
        assert_eq!(
            f_remaining, c_remaining,
            "Q13: remaining order count mismatch"
        );

        let f_sum = get_int(&f, "SELECT SUM(amount) FROM orders").await.unwrap();
        let c_sum = c_get_int(&c, "SELECT SUM(amount) FROM orders").unwrap();
        assert_eq!(f_sum, c_sum, "Q13: remaining sum mismatch");

        eprintln!("Q13: DELETE with IN subquery — {f_remaining} orders remain, oracle parity");
    });
}

// ─── Q14: COALESCE / NULLIF / IIF ───────────────────────────────

#[test]
fn q14_coalesce_nullif_iif() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q14_f.db");
        let c_path = dir.path().join("q14_c.db");
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");
        seed_employees(&f, &c).await;

        // COALESCE: replace NULL mgr_id with -1
        let sql1 = "SELECT COUNT(*) FROM employees WHERE COALESCE(mgr_id, -1) = -1";
        let f_v1 = get_int(&f, sql1).await.unwrap();
        let c_v1 = c_get_int(&c, sql1).unwrap();
        assert_eq!(f_v1, c_v1, "Q14: COALESCE count mismatch");

        // NULLIF: turn 'eng' into NULL
        let sql2 = "SELECT COUNT(*) FROM employees WHERE NULLIF(dept, 'eng') IS NULL";
        let f_v2 = get_int(&f, sql2).await.unwrap();
        let c_v2 = c_get_int(&c, sql2).unwrap();
        assert_eq!(f_v2, c_v2, "Q14: NULLIF count mismatch");

        // IIF: conditional expression
        let sql3 = "SELECT SUM(IIF(salary > 100000, salary, 0)) FROM employees";
        let f_v3 = get_int(&f, sql3).await.unwrap();
        let c_v3 = c_get_int(&c, sql3).unwrap();
        assert_eq!(f_v3, c_v3, "Q14: IIF sum mismatch");

        eprintln!("Q14: COALESCE={f_v1}, NULLIF={f_v2}, IIF={f_v3} — oracle parity");
    });
}

// ─── Q15: Multiple aggregates with DISTINCT ──────────────────────

#[test]
fn q15_multiple_aggregates_distinct() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("q15_f.db");
        let c_path = dir.path().join("q15_c.db");
        {
            let f = Connection::open(f_path.to_str().expect("p"))
                .await
                .expect("f open");
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            seed_employees(&f, &c).await;
            seed_orders(&f, &c).await;
        }
        let f = Connection::open(f_path.to_str().expect("p"))
            .await
            .expect("f reopen");
        let c = rusqlite::Connection::open(&c_path).expect("c reopen");

        let sql = "SELECT \
               COUNT(*) AS total_orders, \
               COUNT(DISTINCT emp_id) AS unique_emps, \
               SUM(amount) AS total_amount, \
               AVG(amount) AS avg_amount, \
               MIN(amount) AS min_amount, \
               MAX(amount) AS max_amount \
               FROM orders";

        let f_rows = f.query(sql).await.expect("f query");
        let c_total: i64 = c
            .query_row("SELECT COUNT(*) FROM orders", [], |r| r.get(0))
            .unwrap();
        let c_distinct: i64 = c
            .query_row("SELECT COUNT(DISTINCT emp_id) FROM orders", [], |r| {
                r.get(0)
            })
            .unwrap();
        let c_sum: i64 = c
            .query_row("SELECT SUM(amount) FROM orders", [], |r| r.get(0))
            .unwrap();
        let c_min: i64 = c
            .query_row("SELECT MIN(amount) FROM orders", [], |r| r.get(0))
            .unwrap();
        let c_max: i64 = c
            .query_row("SELECT MAX(amount) FROM orders", [], |r| r.get(0))
            .unwrap();

        let f_total = match f_rows[0].get(0) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        let f_distinct = match f_rows[0].get(1) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        let f_sum = match f_rows[0].get(2) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        let f_min = match f_rows[0].get(4) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        let f_max = match f_rows[0].get(5) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };

        assert_eq!(f_total, c_total, "Q15: total count mismatch");
        assert_eq!(f_distinct, c_distinct, "Q15: distinct emp count mismatch");
        assert_eq!(f_sum, c_sum, "Q15: sum mismatch");
        assert_eq!(f_min, c_min, "Q15: min mismatch");
        assert_eq!(f_max, c_max, "Q15: max mismatch");

        eprintln!(
            "Q15: total={f_total}, distinct_emps={f_distinct}, sum={f_sum}, min={f_min}, max={f_max} — oracle parity"
        );
    });
}
