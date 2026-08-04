//! bd-qdjhj — Oracle-parity e2e: common table expressions (recursive and not)
//! vs rusqlite (real SQLite).
//!
//! Recursive CTEs are a dense divergence source: the initial-vs-recursive split,
//! UNION (dedup) vs UNION ALL (keep) recursion semantics, the recursive
//! reference seeing only the prior iteration, LIMIT terminating an otherwise
//! infinite recursion, and accumulation patterns (series, fibonacci, tree
//! traversal with depth/path). Also covers non-recursive niceties: chained
//! CTEs, a CTE referenced multiple times, a CTE used in a JOIN, and a CTE name
//! that shadows a real table. All data is fixed and deterministic.
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

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let fconn = Connection::open(":memory:").await.expect("open frank");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        fconn
            .execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        rconn
            .execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (fconn, rconn)
}

async fn assert_scalar_parity(queries: &[&str], label: &str) {
    let fconn = Connection::open(":memory:").await.expect("open frank");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&fconn, &rconn, queries, label).await;
}

async fn check(fconn: &Connection, rconn: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(fconn, q).await, sqlite_rows(rconn, q)) {
            (Ok(f), Ok(s)) if f == s => {}
            (Ok(f), Ok(s)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {f:?}\n  csql:  {s:?}"));
            }
            (Err(fe), Ok(s)) => {
                mismatches.push(format!(
                    "FRANK_ERR: {q}\n  frank: ERROR({fe})\n  csql:  {s:?}"
                ));
            }
            (Ok(f), Err(se)) => {
                mismatches.push(format!(
                    "CSQL_ERR: {q}\n  frank: {f:?}\n  csql: ERROR({se})"
                ));
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
fn cte_recursive_counting_series() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 10) \
                 SELECT n FROM c ORDER BY n",
                // Step by 2.
                "WITH RECURSIVE c(n) AS (SELECT 0 UNION ALL SELECT n+2 FROM c WHERE n < 10) \
                 SELECT n FROM c ORDER BY n",
                // Descending recursion.
                "WITH RECURSIVE c(n) AS (SELECT 5 UNION ALL SELECT n-1 FROM c WHERE n > 1) \
                 SELECT n FROM c ORDER BY n",
                // Aggregate over the series.
                "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 100) \
                 SELECT sum(n), count(*), max(n) FROM c",
            ],
            "cte_recursive_counting_series",
        )
        .await;
    });
}

#[test]
fn cte_union_vs_union_all_recursion() {
    // A recursion that revisits values: UNION dedups (terminates), UNION ALL
    // would diverge, so the UNION ALL variant is bounded by a guard.
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                // UNION dedups: each residue class mod 3 visited once.
                "WITH RECURSIVE c(n) AS (SELECT 0 UNION SELECT (n+1) % 3 FROM c) \
                 SELECT n FROM c ORDER BY n",
                // UNION ALL with an explicit bound keeps duplicates.
                "WITH RECURSIVE c(n, i) AS \
                 (SELECT 7, 0 UNION ALL SELECT 7, i+1 FROM c WHERE i < 3) \
                 SELECT n, i FROM c ORDER BY i",
            ],
            "cte_union_vs_union_all_recursion",
        )
        .await;
    });
}

#[test]
fn cte_recursive_fibonacci() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "WITH RECURSIVE fib(a, b) AS \
                 (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 200) \
                 SELECT a FROM fib ORDER BY a",
                // Factorial accumulation.
                "WITH RECURSIVE f(n, acc) AS \
                 (SELECT 1, 1 UNION ALL SELECT n+1, acc*(n+1) FROM f WHERE n < 6) \
                 SELECT n, acc FROM f ORDER BY n",
            ],
            "cte_recursive_fibonacci",
        )
        .await;
    });
}

#[test]
fn cte_recursive_limit_terminates() {
    // LIMIT must halt an otherwise unbounded recursion.
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c) \
                 SELECT n FROM c LIMIT 5",
                "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c) \
                 SELECT n FROM c LIMIT 3 OFFSET 2",
            ],
            "cte_recursive_limit_terminates",
        )
        .await;
    });
}

#[test]
fn cte_recursive_tree_traversal() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE org (id INTEGER PRIMARY KEY, parent INTEGER, name TEXT)",
            "INSERT INTO org VALUES \
             (1,NULL,'ceo'),(2,1,'vp_eng'),(3,1,'vp_sales'),(4,2,'lead'),(5,4,'dev'),(6,3,'rep')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Depth + path from root.
                "WITH RECURSIVE tree(id, name, depth, path) AS ( \
                   SELECT id, name, 0, name FROM org WHERE parent IS NULL \
                   UNION ALL \
                   SELECT o.id, o.name, t.depth+1, t.path || '/' || o.name \
                   FROM org o JOIN tree t ON o.parent = t.id) \
                 SELECT id, depth, path FROM tree ORDER BY path",
                // Subtree under a given node.
                "WITH RECURSIVE sub(id) AS ( \
                   SELECT id FROM org WHERE id = 2 \
                   UNION ALL \
                   SELECT o.id FROM org o JOIN sub s ON o.parent = s.id) \
                 SELECT id FROM sub ORDER BY id",
                // count of descendants per root child.
                "WITH RECURSIVE sub(id) AS ( \
                   SELECT id FROM org WHERE id = 1 \
                   UNION ALL SELECT o.id FROM org o JOIN sub s ON o.parent = s.id) \
                 SELECT count(*) FROM sub",
            ],
            "cte_recursive_tree_traversal",
        )
        .await;
    });
}

#[test]
fn cte_chained_reused_and_join() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, v INTEGER)",
            "INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',30)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Chained: second CTE references the first.
                "WITH sums AS (SELECT grp, sum(v) AS s FROM t GROUP BY grp), \
                      big AS (SELECT grp FROM sums WHERE s >= 25) \
                 SELECT grp FROM big ORDER BY grp",
                // A CTE referenced twice (self-join on the CTE).
                "WITH g AS (SELECT grp, sum(v) AS s FROM t GROUP BY grp) \
                 SELECT a.grp, b.grp FROM g a JOIN g b ON a.s = b.s AND a.grp < b.grp ORDER BY a.grp",
                // CTE joined with a base table.
                "WITH avg_by_grp AS (SELECT grp, avg(v) AS av FROM t GROUP BY grp) \
                 SELECT t.id, t.v, a.av FROM t JOIN avg_by_grp a ON t.grp = a.grp \
                 WHERE t.v > a.av ORDER BY t.id",
            ],
            "cte_chained_reused_and_join",
        )
        .await;
    });
}

#[test]
fn cte_shadows_table_name() {
    // A CTE named like a real table shadows it within the statement.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE nums (n INTEGER)",
            "INSERT INTO nums VALUES (100),(200),(300)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "WITH nums(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) \
                 SELECT n FROM nums ORDER BY n",
                // Without the CTE, the base table is visible.
                "SELECT n FROM nums ORDER BY n",
            ],
            "cte_shadows_table_name",
        )
        .await;
    });
}
