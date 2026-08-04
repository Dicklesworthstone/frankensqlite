//! bd-bwahg — Oracle-parity e2e: recursive CTE edge cases vs rusqlite.
//!
//! cte_oracle_e2e covers the basic recursive forms (counting series, UNION vs
//! UNION ALL, fibonacci, LIMIT termination, tree traversal). This adds the
//! corners it omits: cycle termination via UNION (de-dup stops a cyclic graph
//! walk that UNION ALL would loop on forever), multiple anchor rows feeding the
//! recursion, a running computed value (factorial), LIMIT cutting an otherwise
//! infinite recursion, and the error when the recursive term uses an aggregate.
//! Each scenario asserts per-statement agreement, then compares query results.

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

#[test]
fn recursive_cycle_terminates_via_union() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE edges (a INTEGER, b INTEGER)",
            "INSERT INTO edges VALUES (1,2),(2,3),(3,1),(3,4)", // 1->2->3->1 cycle, 3->4
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // UNION de-dups, so the cycle terminates; reachable from 1 = {1,2,3,4}.
                "WITH RECURSIVE reach(n) AS (SELECT 1 UNION SELECT b FROM edges JOIN reach ON edges.a = reach.n) \
                 SELECT n FROM reach ORDER BY n",
                "WITH RECURSIVE reach(n) AS (SELECT 1 UNION SELECT b FROM edges JOIN reach ON edges.a = reach.n) \
                 SELECT count(*) FROM reach", // 4
            ],
            "recursive_cycle_terminates_via_union",
        )
        .await;
    });
}

#[test]
fn recursive_multiple_anchor_rows() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        check(
            &f,
            &r,
            &[
                // Two anchors (1 and 10); only values <3 recurse. -> 1,2,3,10.
                "WITH RECURSIVE c(n) AS (VALUES (1),(10) UNION ALL SELECT n+1 FROM c WHERE n < 3) \
                 SELECT n FROM c ORDER BY n",
            ],
            "recursive_multiple_anchor_rows",
        )
        .await;
    });
}

#[test]
fn recursive_factorial_running_value() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        check(
            &f,
            &r,
            &[
                "WITH RECURSIVE f(n, fact) AS (SELECT 1, 1 UNION ALL SELECT n+1, fact*(n+1) FROM f WHERE n < 5) \
                 SELECT n, fact FROM f ORDER BY n", // (1,1),(2,2),(3,6),(4,24),(5,120)
            ],
            "recursive_factorial_running_value",
        )
        .await;
    });
}

#[test]
fn recursive_limit_cuts_infinite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        check(
            &f,
            &r,
            &[
                // No WHERE termination; LIMIT cuts the otherwise-infinite recursion.
                "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c) SELECT n FROM c LIMIT 5", // 1..5
                "WITH RECURSIVE c(n) AS (SELECT 0 UNION ALL SELECT n+2 FROM c) SELECT n FROM c LIMIT 4", // 0,2,4,6
            ],
            "recursive_limit_cuts_infinite",
        )
        .await;
    });
}

/// bd-fuxgg (recursive extension): SQLite rejects an aggregate in a recursive
/// term ("recursive aggregate queries not supported"); frank runs the recursion
/// instead. Same missing-aggregate-validation class as the other misuse cases.
#[test]
fn recursive_aggregate_in_recursive_term_errors() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        check(
            &f,
            &r,
            &[
                // An aggregate in the recursive term is not allowed -> error on both.
                "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT count(*) FROM c) SELECT * FROM c",
            ],
            "recursive_aggregate_in_recursive_term_errors",
        )
        .await;
    });
}
