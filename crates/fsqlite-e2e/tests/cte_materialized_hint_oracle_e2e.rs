//! bd-fvzl9 — Oracle-parity e2e: CTE materialization hints vs rusqlite.
//!
//! SQLite 3.35 lets a non-recursive CTE be annotated `AS MATERIALIZED (...)` or
//! `AS NOT MATERIALIZED (...)` to steer the planner (materialize into a transient
//! table vs inline/flatten into the outer query). The hint is a *planning* knob
//! only — it must never change the result set. cte_oracle covers recursive and
//! plain CTEs; nothing exercises the hints. These confirm both hints parse and
//! execute, across single-ref, multi-ref (self-join), aggregate, and chained
//! mixed-hint CTEs, with results identical to rusqlite.
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

async fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in [
        "CREATE TABLE nums (n INTEGER)",
        "INSERT INTO nums VALUES (1),(2),(3),(4),(5),(6)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"));
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"));
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
fn cte_materialized_single_ref() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        check(
            &f,
            &r,
            &[
                "WITH evens AS MATERIALIZED (SELECT n FROM nums WHERE n % 2 = 0) \
                 SELECT n FROM evens ORDER BY n", // 2,4,6
                "WITH evens AS MATERIALIZED (SELECT n FROM nums WHERE n % 2 = 0) \
                 SELECT count(*), sum(n) FROM evens", // 3, 12
            ],
            "cte_materialized_single_ref",
        )
        .await;
    });
}

#[test]
fn cte_not_materialized_single_ref() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        check(
            &f,
            &r,
            &[
                "WITH odds AS NOT MATERIALIZED (SELECT n FROM nums WHERE n % 2 = 1) \
                 SELECT n FROM odds ORDER BY n DESC", // 5,3,1
                "WITH odds AS NOT MATERIALIZED (SELECT n FROM nums WHERE n % 2 = 1) \
                 SELECT n FROM odds WHERE n > 1 ORDER BY n", // 3,5
            ],
            "cte_not_materialized_single_ref",
        )
        .await;
    });
}

#[test]
fn cte_materialized_multiple_refs_self_join() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // A CTE referenced twice (self-join) — the case where MATERIALIZED is most
        // likely to influence the plan; the rows must still be exact.
        check(
            &f,
            &r,
            &[
                "WITH e AS MATERIALIZED (SELECT n FROM nums WHERE n <= 3) \
                 SELECT a.n, b.n FROM e a JOIN e b ON a.n < b.n ORDER BY a.n, b.n",
                // (1,2),(1,3),(2,3)
                "WITH e AS MATERIALIZED (SELECT n FROM nums WHERE n <= 3) \
                 SELECT count(*) FROM e a JOIN e b ON a.n <> b.n", // 6 ordered pairs
            ],
            "cte_materialized_multiple_refs_self_join",
        )
        .await;
    });
}

#[test]
fn cte_materialized_aggregate_and_chained_mixed() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        check(
            &f,
            &r,
            &[
                // Aggregate inside a NOT MATERIALIZED CTE, filtered by the outer query.
                "WITH agg AS NOT MATERIALIZED \
                   (SELECT n % 2 AS parity, count(*) AS c, sum(n) AS s FROM nums GROUP BY n % 2) \
                 SELECT parity, c, s FROM agg ORDER BY parity", // (0,3,12),(1,3,9)
                // Chained CTEs with mixed hints: b reads from materialized a.
                "WITH a AS MATERIALIZED (SELECT n FROM nums WHERE n >= 3), \
                      b AS NOT MATERIALIZED (SELECT n * 10 AS m FROM a) \
                 SELECT m FROM b ORDER BY m", // 30,40,50,60
            ],
            "cte_materialized_aggregate_and_chained_mixed",
        )
        .await;
    });
}
