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

/// bd-reality-check-1022-llk6g.15: an ORDER BY on the recursive compound makes
/// the queue a priority queue. Stock takes one row at a time, the first in
/// that order (ties first-in first-out), and queues what the recursive arms
/// produce from it. frank used to run whole generations: breadth-first.
#[test]
fn recursive_order_by_sets_queue_priority() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE org (name TEXT PRIMARY KEY, boss TEXT)",
            "INSERT INTO org VALUES ('A',NULL),('Ax','A'),('Ay','A'),('Axx','Ax'),('Axy','Ax'),('Ayx','Ay')",
            "CREATE TABLE fam (name TEXT, parent TEXT, born INT)",
            "INSERT INTO fam VALUES ('Root',NULL,1900),('B','Root',1930),('A','Root',1925),\
             ('Ba','B',1960),('Aa','A',1950),('Ab','A',1955)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Depth-first (the SQLite docs' org chart): A,Ax,Axx,Axy,Ay,Ayx.
                "WITH RECURSIVE u(name, lvl) AS (SELECT 'A', 0 UNION ALL \
                 SELECT org.name, u.lvl + 1 FROM org JOIN u ON org.boss = u.name ORDER BY 2 DESC) \
                 SELECT group_concat(name, ',') FROM u",
                "WITH RECURSIVE u(name, lvl) AS (SELECT 'A', 0 UNION ALL \
                 SELECT org.name, u.lvl + 1 FROM org JOIN u ON org.boss = u.name ORDER BY 2) \
                 SELECT group_concat(name, ',') FROM u",
                // The ORDER BY term binds to an alias of the first SELECT.
                "WITH RECURSIVE u(name, lvl) AS (SELECT 'A' AS name, 0 AS lvl UNION ALL \
                 SELECT org.name, u.lvl + 1 FROM org JOIN u ON org.boss = u.name ORDER BY lvl DESC) \
                 SELECT group_concat(name, ',') FROM u",
                "WITH RECURSIVE u(name, lvl) AS (SELECT 'A', 0 UNION ALL \
                 SELECT org.name, u.lvl + 1 FROM org JOIN u ON org.boss = u.name ORDER BY 2 DESC, 1 DESC) \
                 SELECT group_concat(name, ',') FROM u",
                "WITH RECURSIVE t(name, depth, born) AS (SELECT 'Root', 0, 1900 UNION ALL \
                 SELECT fam.name, t.depth + 1, fam.born FROM fam JOIN t ON fam.parent = t.name \
                 ORDER BY 2 DESC, 3) SELECT group_concat(name) FROM t",
                "WITH RECURSIVE t(name, depth, born) AS (SELECT 'Root', 0, 1900 UNION ALL \
                 SELECT fam.name, t.depth + 1, fam.born FROM fam JOIN t ON fam.parent = t.name \
                 ORDER BY 3 DESC) SELECT group_concat(name) FROM t",
                // Several seed rows: 1,0,3,2,1,0,5,4,3,2,1,0 (not 1,3,5,0,2,4,...).
                "WITH RECURSIVE c(x) AS (VALUES (5),(1),(3) UNION ALL \
                 SELECT x - 1 FROM c WHERE x > 0 ORDER BY 1) SELECT group_concat(x) FROM c",
                "WITH RECURSIVE c(x) AS (VALUES (5),(1),(3) UNION \
                 SELECT x - 1 FROM c WHERE x > 0 ORDER BY 1 DESC) SELECT group_concat(x) FROM c",
                "WITH RECURSIVE c(x) AS (VALUES (3),(1),(2) UNION ALL \
                 SELECT x + 10 FROM c WHERE x < 10 ORDER BY 1 NULLS LAST) SELECT group_concat(x) FROM c",
                // Collation ties leave first-in first-out: A,a,A.,a.,b,b.
                "WITH RECURSIVE c(x) AS (VALUES ('b'),('A'),('a') UNION ALL \
                 SELECT x || '.' FROM c WHERE length(x) < 2 ORDER BY 1 COLLATE NOCASE) \
                 SELECT group_concat(x) FROM c",
                // NULL sorts low, so DESC takes it last.
                "WITH RECURSIVE c(x) AS (VALUES (NULL),(1) UNION ALL \
                 SELECT x + 1 FROM c WHERE x < 2 ORDER BY 1 DESC) \
                 SELECT group_concat(coalesce(x, 'N')) FROM c",
                "WITH RECURSIVE c(x, y) AS (SELECT 1, 'z' AS y UNION ALL \
                 SELECT x + 1, char(120 - x) FROM c WHERE x < 4 ORDER BY y) \
                 SELECT group_concat(x || y) FROM c",
                // Two recursive arms feed one queue, with and without UNION dedup.
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x * 2 FROM c WHERE x < 20 \
                 UNION ALL SELECT x * 3 FROM c WHERE x < 20 ORDER BY 1) SELECT group_concat(x) FROM c",
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION SELECT x * 2 FROM c WHERE x < 20 \
                 UNION SELECT x * 3 FROM c WHERE x < 20 ORDER BY 1 DESC) SELECT group_concat(x) FROM c",
                // LIMIT / OFFSET count rows in queue order: A,Ax,Axx and Ax,Axx,Axy.
                "WITH RECURSIVE u(name, lvl) AS (SELECT 'A', 0 UNION ALL \
                 SELECT org.name, u.lvl + 1 FROM org JOIN u ON org.boss = u.name ORDER BY 2 DESC LIMIT 3) \
                 SELECT group_concat(name, ',') FROM u",
                "WITH RECURSIVE u(name, lvl) AS (SELECT 'A', 0 UNION ALL \
                 SELECT org.name, u.lvl + 1 FROM org JOIN u ON org.boss = u.name \
                 ORDER BY 2 DESC LIMIT 3 OFFSET 1) SELECT group_concat(name, ',') FROM u",
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 9 \
                 ORDER BY 1 DESC LIMIT 4 OFFSET 2) SELECT group_concat(x) FROM c",
                // A term that names no result column is an error on both.
                "WITH RECURSIVE u(name, lvl) AS (SELECT 'A', 0 UNION ALL \
                 SELECT org.name, u.lvl + 1 FROM org JOIN u ON org.boss = u.name ORDER BY lvl DESC) \
                 SELECT group_concat(name, ',') FROM u",
            ],
            "recursive_order_by_sets_queue_priority",
        )
        .await;
    });
}

/// bd-reality-check-1022-llk6g.15: the CTE's own LIMIT and OFFSET, in any
/// form, apply to its output. frank used to honor only a bare literal LIMIT
/// and returned every row for `LIMIT 5 OFFSET 2`.
#[test]
fn recursive_limit_offset_apply_to_cte_output() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        check(
            &f,
            &r,
            &[
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 10 \
                 LIMIT 5 OFFSET 2) SELECT group_concat(x) FROM c", // 3,4,5,6,7
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5 \
                 LIMIT 3, 2) SELECT group_concat(x) FROM c", // 4,5
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5 \
                 LIMIT -1 OFFSET -3) SELECT group_concat(x) FROM c", // 1..5
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5 \
                 LIMIT 0) SELECT count(*) FROM c",
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5 \
                 LIMIT 2 OFFSET 10) SELECT count(*) FROM c",
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5 \
                 LIMIT 1 + 1 OFFSET (SELECT 1)) SELECT group_concat(x) FROM c", // 2,3
                // Not an integer: datatype mismatch on both.
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5 \
                 LIMIT 'a') SELECT count(*) FROM c",
            ],
            "recursive_limit_offset_apply_to_cte_output",
        )
        .await;
    });
}

/// bd-reality-check-1022-llk6g.15: LIMIT with OFFSET, or a LIMIT that is not
/// a bare literal, must still end an otherwise infinite recursion (frank used
/// to run forever), with and without an ORDER BY.
#[test]
fn recursive_limit_offset_cuts_infinite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[]).await;
        check(
            &f,
            &r,
            &[
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 4 OFFSET 3) \
                 SELECT group_concat(x) FROM c", // 4,5,6,7
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
                 LIMIT 2 + 2 OFFSET (SELECT 3)) SELECT group_concat(x) FROM c",
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c ORDER BY 1 DESC LIMIT 5) \
                 SELECT group_concat(x) FROM c",
                // The consumer's LIMIT bounds an ordered recursion too.
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c ORDER BY 1 DESC) \
                 SELECT x FROM c LIMIT 3",
            ],
            "recursive_limit_offset_cuts_infinite",
        )
        .await;
    });
}
