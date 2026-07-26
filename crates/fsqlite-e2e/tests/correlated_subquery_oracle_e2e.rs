//! bd-ay6ji — Oracle-parity e2e: correlated subqueries vs rusqlite.
//!
//! subquery_oracle has one correlated-scalar case; this exercises correlation
//! across positions and shapes: a correlated scalar subquery in the SELECT list
//! (per-row aggregate of related rows), correlated EXISTS / NOT EXISTS in WHERE,
//! a correlated IN subquery (top-per-group), a self-correlated comparison
//! (rows above their own group's average), and doubly-nested correlation (an
//! inner subquery referencing two enclosing levels). Each scenario compares
//! query results against rusqlite; row order is pinned with ORDER BY and the
//! data is chosen so all averages are exact integers.
#![recursion_limit = "512"]

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

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in stmts {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

// dept sizes eng=3 sales=2 hr=1; dept avgs eng=100 sales=70 hr=150 (all exact);
// dept max eng=120 sales=80 hr=150.
fn emp() -> [&'static str; 2] {
    [
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)",
        "INSERT INTO emp VALUES (1,'eng',100),(2,'eng',120),(3,'sales',80),(4,'sales',60),(5,'eng',80),(6,'hr',150)",
    ]
}

#[test]
fn correlated_scalar_in_select_list() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&emp()).await;
        check(
            &f,
            &r,
            &[
                // Per-row count of same-dept rows.
                "SELECT id, (SELECT count(*) FROM emp e2 WHERE e2.dept = e1.dept) AS sz FROM emp e1 ORDER BY id",
                // Per-row dept average (exact integers -> rendered as whole numbers).
                "SELECT id, (SELECT avg(salary) FROM emp e2 WHERE e2.dept = e1.dept) AS a FROM emp e1 ORDER BY id",
            ],
            "correlated_scalar_in_select_list",
        )
        .await;
    });
}

#[test]
fn correlated_exists_and_not_exists() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, cust_id INTEGER)",
            "INSERT INTO customers VALUES (1,'a'),(2,'b'),(3,'c')",
            "INSERT INTO orders VALUES (10,1),(11,1),(12,3)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust_id = c.id) ORDER BY id", // 1,3
                "SELECT id FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.cust_id = c.id) ORDER BY id", // 2
            ],
            "correlated_exists_and_not_exists",
        )
        .await;
    });
}

/// bd-zvk68(A): a correlated IN-subquery returns no rows, though the identical
/// correlated+aggregate subquery via scalar `=` works and a non-correlated IN
/// works. Correlation is not threaded into the `x IN (SELECT ...)` path.
#[test]
fn correlated_in_top_per_group() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&emp()).await;
        check(
            &f,
            &r,
            &[
                // Top earner(s) per dept: salary == max(salary) within the same dept.
                "SELECT id FROM emp e1 WHERE salary IN (SELECT max(salary) FROM emp e2 WHERE e2.dept = e1.dept) ORDER BY id", // 2,3,6
            ],
            "correlated_in_top_per_group",
        )
        .await;
    });
}

#[test]
fn self_correlated_above_group_average() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&emp()).await;
        check(
            &f,
            &r,
            &[
                // Rows strictly above their own dept's average (eng>100->id2; sales>70->id3; hr none).
                "SELECT id, salary FROM emp e1 WHERE salary > (SELECT avg(salary) FROM emp e2 WHERE e2.dept = e1.dept) ORDER BY id", // 2,3
            ],
            "self_correlated_above_group_average",
        )
        .await;
    });
}

/// bd-zvk68(B): the innermost EXISTS is not bound to the middle query's row, so
/// it evaluates true regardless and the outer query over-matches. Single-level
/// correlated EXISTS works (correlated_exists_and_not_exists).
#[test]
fn doubly_nested_correlation() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (id INTEGER PRIMARY KEY)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, aid INTEGER)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, bid INTEGER)",
            "INSERT INTO a VALUES (1),(2),(3)",
            "INSERT INTO b VALUES (10,1),(11,2)",
            "INSERT INTO c VALUES (100,10)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // a qualifies iff it has a b that itself has a c. Only a=1.
                "SELECT id FROM a WHERE EXISTS (\
                   SELECT 1 FROM b WHERE b.aid = a.id AND EXISTS (\
                     SELECT 1 FROM c WHERE c.bid = b.id)) ORDER BY id",
            ],
            "doubly_nested_correlation",
        )
        .await;
    });
}
