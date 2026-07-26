//! bd-nhfpo — Oracle-parity e2e: subquery semantics vs rusqlite (real SQLite).
//!
//! Focuses on the value/row rules: a scalar subquery yields NULL when it
//! returns no rows and the first row's value when it returns several (SQLite
//! does NOT error on a multi-row scalar subquery), correlated scalar subqueries,
//! EXISTS/NOT EXISTS (two-valued, correlated), IN/NOT IN against a subquery
//! (incl. the NULL trap), multi-column row-value IN against a subquery, and
//! nested subqueries. Deterministic data; row order pinned with ORDER BY.

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
        let frank = frank_rows(f, q).await;
        match (frank, sqlite_rows(r, q)) {
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

fn two_tables() -> [&'static str; 4] {
    [
        "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT, salary INTEGER)",
        "INSERT INTO dept VALUES (1,'eng'),(2,'sales'),(3,'empty')",
        "INSERT INTO emp VALUES \
         (1,1,'ann',100),(2,1,'bob',200),(3,2,'cy',150),(4,2,'dee',150),(5,1,'eve',300)",
    ]
}

#[test]
fn scalar_subquery_empty_and_single() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                // Empty subquery -> NULL.
                "SELECT (SELECT name FROM dept WHERE id = 99)",
                "SELECT (SELECT max(salary) FROM emp WHERE dept_id = 3)", // no rows -> NULL via aggregate
                // Single-row scalar subquery.
                "SELECT (SELECT name FROM dept WHERE id = 1)",
                "SELECT (SELECT count(*) FROM emp)",
                // Scalar subquery in arithmetic.
                "SELECT (SELECT max(salary) FROM emp) - (SELECT min(salary) FROM emp)",
            ],
            "scalar_subquery_empty_and_single",
        )
        .await;
    });
}

#[test]
fn scalar_subquery_multi_row_takes_first() {
    // SQLite does not error on a multi-row scalar subquery; it uses the first
    // row in scan (rowid) order. Pin it both ways: the bare form and an
    // explicitly ordered form.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                "SELECT (SELECT salary FROM emp WHERE dept_id = 1)", // first eng row (rowid order)
                "SELECT (SELECT salary FROM emp WHERE dept_id = 1 ORDER BY salary DESC)",
                "SELECT (SELECT name FROM emp ORDER BY salary)", // lowest-paid name
            ],
            "scalar_subquery_multi_row_takes_first",
        )
        .await;
    });
}

#[test]
fn correlated_scalar_subquery() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                // Per-dept max salary via correlated scalar subquery.
                "SELECT d.name, (SELECT max(e.salary) FROM emp e WHERE e.dept_id = d.id) \
                 FROM dept d ORDER BY d.id",
                // Per-employee: how many earn more in the same dept.
                "SELECT e.name, (SELECT count(*) FROM emp x WHERE x.dept_id = e.dept_id AND x.salary > e.salary) \
                 FROM emp e ORDER BY e.id",
            ],
            "correlated_scalar_subquery",
        )
        .await;
    });
}

#[test]
fn exists_not_exists() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                // Depts that have employees.
                "SELECT name FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id) ORDER BY d.id",
                // Depts with no employees (the 'empty' dept).
                "SELECT name FROM dept d WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id) ORDER BY d.id",
                // EXISTS with an additional predicate.
                "SELECT name FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary > 250) ORDER BY d.id",
            ],
            "exists_not_exists",
        )
        .await;
    });
}

#[test]
fn in_and_not_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                "SELECT name FROM dept WHERE id IN (SELECT dept_id FROM emp) ORDER BY id",
                "SELECT name FROM dept WHERE id NOT IN (SELECT dept_id FROM emp WHERE salary > 250) ORDER BY id",
                // Subquery on emp salaries.
                "SELECT name FROM emp WHERE salary IN (SELECT salary FROM emp WHERE dept_id = 2) ORDER BY id",
                // NOT IN against a subquery that yields no NULLs.
                "SELECT name FROM emp WHERE dept_id NOT IN (SELECT id FROM dept WHERE name = 'sales') ORDER BY id",
            ],
            "in_and_not_in_subquery",
        )
        .await;
    });
}

#[test]
fn not_in_subquery_null_trap() {
    // A NULL in the NOT IN subquery result makes the whole predicate yield no
    // rows (the classic trap), as a correlated subquery as well.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO a VALUES (1,10),(2,20),(3,30)",
            "INSERT INTO b VALUES (1,10),(2,NULL)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id FROM a WHERE v NOT IN (SELECT v FROM b) ORDER BY id", // NULL -> no rows
                "SELECT id FROM a WHERE v IN (SELECT v FROM b) ORDER BY id",     // matches v=10
                // Removing the NULL row restores normal NOT IN.
                "SELECT id FROM a WHERE v NOT IN (SELECT v FROM b WHERE v IS NOT NULL) ORDER BY id",
            ],
            "not_in_subquery_null_trap",
        )
        .await;
    });
}

#[test]
fn nested_subqueries() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                // Nested subqueries: depts whose max salary exceeds the overall avg.
                "SELECT name FROM dept WHERE id IN ( \
                   SELECT dept_id FROM emp GROUP BY dept_id \
                   HAVING max(salary) > (SELECT avg(salary) FROM emp)) ORDER BY id",
                // Three-level nesting.
                "SELECT name FROM emp WHERE salary = ( \
                   SELECT max(salary) FROM emp WHERE dept_id = ( \
                     SELECT id FROM dept WHERE name = 'eng')) ORDER BY id",
            ],
            "nested_subqueries",
        )
        .await;
    });
}

/// Row-value IN against a subquery RHS is broken (matches nothing); tracked in
/// bd-7ccda. Row-value IN against a literal list works.
#[test]
fn multi_column_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                "SELECT name FROM emp WHERE (dept_id, salary) IN (SELECT dept_id, max(salary) FROM emp GROUP BY dept_id) ORDER BY id",
            ],
            "multi_column_in_subquery",
        )
        .await;
    });
}
