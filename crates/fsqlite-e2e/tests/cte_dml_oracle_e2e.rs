//! bd-7tgw0 — Oracle-parity e2e: CTE in DML statements vs rusqlite.
//!
//! cte_oracle_e2e covers WITH in SELECT; this covers a WITH clause attached to a
//! mutation: `WITH ... INSERT ... SELECT`, a recursive CTE generating a series
//! that is inserted into a table, `WITH ... UPDATE` / `WITH ... DELETE` whose
//! predicate selects from the CTE, and a recursive CTE feeding an INSERT with a
//! projection transform + filter. Each scenario asserts per-statement agreement
//! with rusqlite, then compares the resulting table state.
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

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
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

#[test]
fn cte_insert_from_values() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE dst (a INTEGER, b TEXT)",
                "WITH src(a,b) AS (VALUES (1,'x'),(2,'y'),(3,'z')) INSERT INTO dst SELECT a, b FROM src",
            ],
            &["SELECT a, b FROM dst ORDER BY a"], // (1,x),(2,y),(3,z)
            "cte_insert_from_values",
        )
        .await;
    });
}

#[test]
fn recursive_cte_insert_series() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE nums (n INTEGER)",
                "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 5) \
                 INSERT INTO nums SELECT n FROM seq",
            ],
            &[
                "SELECT n FROM nums ORDER BY n",     // 1..5
                "SELECT count(*), sum(n) FROM nums", // 5, 15
            ],
            "recursive_cte_insert_series",
        )
        .await;
    });
}

#[test]
fn cte_update_via_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,5)",
                "WITH big AS (SELECT id FROM t WHERE v > 15) UPDATE t SET v = 0 WHERE id IN (SELECT id FROM big)",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (1,10),(2,0),(3,0),(4,5)
            "cte_update_via_in_subquery",
        )
        .await;
    });
}

#[test]
fn cte_delete_via_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,5)",
                "WITH small AS (SELECT id FROM t WHERE v < 15) DELETE FROM t WHERE id IN (SELECT id FROM small)",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (2,20),(3,30)
            "cte_delete_via_in_subquery",
        )
        .await;
    });
}

#[test]
fn recursive_cte_insert_with_transform_filter() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE squares (n INTEGER, sq INTEGER)",
                "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 6) \
                 INSERT INTO squares SELECT n, n*n FROM seq WHERE n % 2 = 1",
            ],
            &["SELECT n, sq FROM squares ORDER BY n"], // (1,1),(3,9),(5,25)
            "recursive_cte_insert_with_transform_filter",
        )
        .await;
    });
}

#[test]
fn multi_cte_then_insert() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE sales (region TEXT, amt INTEGER)",
                "INSERT INTO sales VALUES ('east',10),('east',20),('west',30),('west',5)",
                "CREATE TABLE report (region TEXT, total INTEGER)",
                "WITH per_region AS (SELECT region, sum(amt) AS s FROM sales GROUP BY region), \
                      big AS (SELECT region, s FROM per_region WHERE s > 25) \
                 INSERT INTO report SELECT region, s FROM big",
            ],
            &["SELECT region, total FROM report ORDER BY region"], // (east,30),(west,35)
            "multi_cte_then_insert",
        )
        .await;
    });
}
