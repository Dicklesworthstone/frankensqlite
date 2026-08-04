//! bd-kcxwe — Oracle-parity e2e: VIEW semantics vs rusqlite (real SQLite).
//!
//! Covers querying simple/filtered/joined/aggregated views, explicit view
//! column aliasing (both `CREATE VIEW v(a,b)` and SELECT aliases), nested views
//! (a view defined over another view), views used in FROM joined with a base
//! table and inside a subquery, and ORDER BY / DISTINCT inside a view
//! definition. All data is fixed; outer ORDER BY pins row order.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

fn base_tables() -> [&'static str; 4] {
    [
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT, salary INTEGER)",
        "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO dept VALUES (1,'eng'),(2,'sales')",
        "INSERT INTO emp VALUES (1,1,'ann',100),(2,1,'bob',200),(3,2,'cy',150),(4,2,'dee',300)",
    ]
}

#[test]
fn view_simple_and_filtered() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            base_tables()[0],
            base_tables()[1],
            base_tables()[2],
            base_tables()[3],
            "CREATE VIEW v_all AS SELECT id, name, salary FROM emp",
            "CREATE VIEW v_highpaid AS SELECT id, name FROM emp WHERE salary >= 200",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id, name, salary FROM v_all ORDER BY id",
                "SELECT * FROM v_all ORDER BY id",
                "SELECT id, name FROM v_highpaid ORDER BY id",
                // Further filtering on top of a view.
                "SELECT name FROM v_all WHERE salary > 150 ORDER BY id",
            ],
            "view_simple_and_filtered",
        )
        .await;
    });
}

#[test]
fn view_select_expression_aliases() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            base_tables()[0],
            base_tables()[1],
            base_tables()[2],
            base_tables()[3],
            // SELECT-expression aliases (these work).
            "CREATE VIEW v_calc AS SELECT id, salary * 12 AS annual, upper(name) AS uname FROM emp",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id, annual, uname FROM v_calc ORDER BY id",
                "SELECT uname FROM v_calc WHERE annual >= 2400 ORDER BY id",
                "SELECT * FROM v_calc ORDER BY id",
            ],
            "view_select_expression_aliases",
        )
        .await;
    });
}

/// Explicit view column-name list `CREATE VIEW v(a,b) AS ...` is ignored
/// (columns keep their underlying names); tracked in bd-ws183.
#[test]
fn view_explicit_column_list() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            base_tables()[0],
            base_tables()[1],
            base_tables()[2],
            base_tables()[3],
            "CREATE VIEW v_named(emp_id, who, pay) AS SELECT id, name, salary FROM emp",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT emp_id, who, pay FROM v_named ORDER BY emp_id",
                "SELECT who FROM v_named WHERE pay >= 200 ORDER BY emp_id",
            ],
            "view_explicit_column_list",
        )
        .await;
    });
}

#[test]
fn view_join_and_aggregate() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            base_tables()[0],
            base_tables()[1],
            base_tables()[2],
            base_tables()[3],
            "CREATE VIEW v_emp_dept AS \
               SELECT e.id, e.name AS emp, d.name AS dept, e.salary \
               FROM emp e JOIN dept d ON e.dept_id = d.id",
            "CREATE VIEW v_dept_totals AS \
               SELECT d.name AS dept, count(*) AS headcount, sum(e.salary) AS payroll \
               FROM dept d JOIN emp e ON e.dept_id = d.id GROUP BY d.id",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id, emp, dept, salary FROM v_emp_dept ORDER BY id",
                "SELECT dept, headcount, payroll FROM v_dept_totals ORDER BY dept",
                "SELECT dept FROM v_dept_totals WHERE payroll > 400 ORDER BY dept",
            ],
            "view_join_and_aggregate",
        )
        .await;
    });
}

#[test]
fn view_nested() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            base_tables()[0],
            base_tables()[1],
            base_tables()[2],
            base_tables()[3],
            "CREATE VIEW v1 AS SELECT id, dept_id, salary FROM emp WHERE salary >= 150",
            // A view defined over another view.
            "CREATE VIEW v2 AS SELECT dept_id, count(*) AS n, max(salary) AS top FROM v1 GROUP BY dept_id",
            // A third level.
            "CREATE VIEW v3 AS SELECT dept_id FROM v2 WHERE n >= 2",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id, dept_id, salary FROM v1 ORDER BY id",
                "SELECT dept_id, n, top FROM v2 ORDER BY dept_id",
                "SELECT dept_id FROM v3 ORDER BY dept_id",
            ],
            "view_nested",
        )
        .await;
    });
}

#[test]
fn view_in_from_and_subquery() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            base_tables()[0],
            base_tables()[1],
            base_tables()[2],
            base_tables()[3],
            "CREATE VIEW v_emp AS SELECT id, dept_id, name, salary FROM emp",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // View joined with a base table.
                "SELECT v.name, d.name FROM v_emp v JOIN dept d ON v.dept_id = d.id ORDER BY v.id",
                // View inside a subquery.
                "SELECT name FROM emp WHERE salary = (SELECT max(salary) FROM v_emp) ORDER BY id",
                // View inside a derived-table FROM.
                "SELECT cnt FROM (SELECT count(*) AS cnt FROM v_emp WHERE salary > 100)",
                // View referenced in IN subquery.
                "SELECT name FROM dept WHERE id IN (SELECT dept_id FROM v_emp WHERE salary >= 300) ORDER BY id",
            ],
            "view_in_from_and_subquery",
        )
        .await;
    });
}

#[test]
fn view_with_distinct_and_order_by() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, v INTEGER)",
            "INSERT INTO t VALUES (1,'a',10),(2,'a',10),(3,'b',20),(4,'b',5),(5,'c',20)",
            "CREATE VIEW v_distinct_cats AS SELECT DISTINCT cat FROM t",
            // ORDER BY inside a view definition.
            "CREATE VIEW v_sorted AS SELECT id, v FROM t ORDER BY v DESC, id",
            "CREATE VIEW v_distinct_v AS SELECT DISTINCT v FROM t",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT cat FROM v_distinct_cats ORDER BY cat",
                "SELECT count(*) FROM v_distinct_cats",
                "SELECT id, v FROM v_sorted ORDER BY v DESC, id",
                "SELECT v FROM v_distinct_v ORDER BY v",
            ],
            "view_with_distinct_and_order_by",
        )
        .await;
    });
}
