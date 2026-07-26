//! bd-eiwpd — Oracle-parity e2e: subquery flattening correctness vs rusqlite.
//!
//! SQLite flattens a subquery into the outer query only when it is safe; when
//! the subquery has LIMIT/OFFSET (or ORDER BY + LIMIT), flattening is suppressed
//! and the outer WHERE must NOT be pushed down into it — the LIMIT picks rows
//! first, then the outer predicate filters. The classic bug is exactly that
//! incorrect predicate pushdown. These pin down that ordering plus a few
//! flatten-eligible shapes (aggregate / DISTINCT / UNION subquery with an outer
//! filter). Compared against rusqlite; row order pinned with ORDER BY.

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
fn outer_where_after_inner_limit() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,20),(3,5),(4,30),(5,15),(6,25)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Inner picks the 3 smallest x (5,10,15); outer filters x>12 -> [15].
                // A wrong WHERE-pushdown would pick 15,20,25 then keep all -> [15,20,25].
                "SELECT x FROM (SELECT x FROM t ORDER BY x LIMIT 3) WHERE x > 12 ORDER BY x",
                // Inner top-3 by x DESC (30,25,20); outer x<28 -> [20,25].
                "SELECT x FROM (SELECT x FROM t ORDER BY x DESC LIMIT 3) WHERE x < 28 ORDER BY x",
                "SELECT count(*) FROM (SELECT x FROM t ORDER BY x LIMIT 4)", // 4
            ],
            "outer_where_after_inner_limit",
        )
        .await;
    });
}

#[test]
fn outer_query_after_inner_limit_offset() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,20),(3,5),(4,30),(5,15),(6,25)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // x DESC LIMIT 2 OFFSET 1 -> 25,20 ; outer ORDER BY x -> [20,25].
                "SELECT x FROM (SELECT x FROM t ORDER BY x DESC LIMIT 2 OFFSET 1) ORDER BY x",
                // x ASC LIMIT 2 OFFSET 2 -> 15,20.
                "SELECT x FROM (SELECT x FROM t ORDER BY x LIMIT 2 OFFSET 2) ORDER BY x",
            ],
            "outer_query_after_inner_limit_offset",
        )
        .await;
    });
}

#[test]
fn flatten_aggregate_subquery_outer_filter() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE s (g TEXT, v INTEGER)",
            "INSERT INTO s VALUES ('a',10),('a',20),('b',5),('c',30),('c',5)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Per-group sums: a=30,b=5,c=35; outer keeps s>25 -> a,c.
                "SELECT g, s FROM (SELECT g, sum(v) AS s FROM s GROUP BY g) WHERE s > 25 ORDER BY g",
            ],
            "flatten_aggregate_subquery_outer_filter",
        )
        .await;
    });
}

#[test]
fn flatten_distinct_subquery_outer_filter() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE d (a INTEGER)",
            "INSERT INTO d VALUES (1),(1),(2),(3),(3),(3),(5)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT a FROM (SELECT DISTINCT a FROM d) WHERE a >= 2 ORDER BY a", // 2,3,5
                "SELECT count(*) FROM (SELECT DISTINCT a FROM d)",                  // 4
            ],
            "flatten_distinct_subquery_outer_filter",
        )
        .await;
    });
}

#[test]
fn flatten_union_subquery_outer_filter() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t1 (a INTEGER)",
            "CREATE TABLE t2 (b INTEGER)",
            "INSERT INTO t1 VALUES (5),(10),(15)",
            "INSERT INTO t2 VALUES (10),(20),(25)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // UNION dedups (10 appears in both); outer keeps v>10.
                "SELECT v FROM (SELECT a AS v FROM t1 UNION SELECT b FROM t2) WHERE v > 10 ORDER BY v", // 15,20,25
            ],
            "flatten_union_subquery_outer_filter",
        )
        .await;
    });
}
