//! Differential oracle: recursive common table expressions (`WITH RECURSIVE`)
//! vs rusqlite (bundled SQLite 3.53). A probe sweep found this surface
//! stock-correct across 12 cases; this keeper locks it in.
//!
//! Covers: bounded counters, LIMIT-terminated unbounded recursion, running
//! accumulation, Fibonacci, hierarchical traversal (descendants, ancestors,
//! path building), graph reachability with UNION dedup, outer-query filters,
//! ORDER BY DESC + LIMIT over the recursive result, and a recursive CTE joined
//! with a non-recursive CTE.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
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
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

const T: &[&str] = &[
    "CREATE TABLE tree(id INT, parent INT, name TEXT)",
    "INSERT INTO tree VALUES (1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'a1'),(5,2,'a2'),(6,3,'b1')",
];

#[test]
fn gh418_sudoku_repeated_on_same_connection_keeps_materialized_roots_live() {
    asupersync::test_utils::run_test(|| async {
        let sql = r"
WITH RECURSIVE
  input(sud) AS (
    VALUES('53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79')
  ),
  digits(z, lp) AS (
    VALUES('1', 1)
    UNION ALL SELECT
    CAST(lp+1 AS TEXT), lp+1 FROM digits WHERE lp<9
  ),
  x(s, ind) AS (
    SELECT sud, instr(sud, '.') FROM input
    UNION ALL
    SELECT
      substr(s, 1, ind-1) || z || substr(s, ind+1),
      instr( substr(s, 1, ind-1) || z || substr(s, ind+1), '.' )
     FROM x, digits AS z
    WHERE ind>0
      AND NOT EXISTS (
            SELECT 1
              FROM digits AS lp
             WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1)
                OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1)
                OR z.z = substr(s, (((ind-1)/3) % 3) * 3
                        + ((ind-1)/27) * 27 + lp
                        + ((lp-1) / 3) * 6, 1)
         )
  )
SELECT s FROM x WHERE ind=0;
";
        let expected =
            "534678912672195348198342567859761423426853791713924856961537284287419635345286179";

        // The CLI asks prepare() for column names before query(), discarding
        // metadata-preparation errors. Cover that ordering and query() alone;
        // the unrecognized `.timer on` never calls the connection.
        for preview_columns in [false, true] {
            let connection = Connection::open(":memory:").await.expect("open");
            for iteration in 1..=2 {
                if preview_columns {
                    let _column_names = connection
                        .prepare(sql)
                        .await
                        .ok()
                        .map(|prepared| prepared.column_names().to_vec());
                }
                let rows = connection.query(sql).await.unwrap_or_else(|error| {
                    panic!(
                        "GH418 iteration {iteration}, preview_columns={preview_columns}: {error}"
                    )
                });
                assert_eq!(
                    rows.len(),
                    1,
                    "GH418 iteration {iteration}, preview_columns={preview_columns}"
                );
                assert_eq!(
                    rows[0].values(),
                    &[SqliteValue::Text(expected.into())],
                    "GH418 iteration {iteration}, preview_columns={preview_columns}"
                );
            }
        }
    });
}

#[test]
fn counters_and_accumulation() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5) SELECT n FROM c ORDER BY n",
              "bounded counter 1..5").await;
        agree(
            &[],
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c) SELECT n FROM c LIMIT 4",
            "unbounded recursion terminated by LIMIT",
        )
        .await;
        agree(&[], "WITH RECURSIVE c(n, tot) AS (SELECT 1, 1 UNION ALL SELECT n+1, tot+n+1 FROM c WHERE n < 5) SELECT n, tot FROM c ORDER BY n",
              "running accumulation").await;
        agree(&[], "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 50) SELECT a FROM fib ORDER BY a",
              "Fibonacci").await;
    });
}

#[test]
fn tree_traversal() {
    asupersync::test_utils::run_test(|| async {
        agree(T, "WITH RECURSIVE d(id, name, depth) AS (SELECT id, name, 0 FROM tree WHERE parent IS NULL UNION ALL SELECT t.id, t.name, d.depth+1 FROM tree t JOIN d ON t.parent = d.id) SELECT depth, name FROM d ORDER BY depth, name",
              "descendants with depth").await;
        agree(T, "WITH RECURSIVE p(id, path) AS (SELECT id, name FROM tree WHERE parent IS NULL UNION ALL SELECT t.id, p.path || '/' || t.name FROM tree t JOIN p ON t.parent = p.id) SELECT path FROM p ORDER BY path",
              "path accumulation").await;
        agree(T, "WITH RECURSIVE up(id, name) AS (SELECT id, name FROM tree WHERE id = 4 UNION ALL SELECT t.id, t.name FROM tree t JOIN up ON t.id = (SELECT parent FROM tree WHERE id = up.id)) SELECT name FROM up ORDER BY id",
              "ancestor walk-up").await;
    });
}

#[test]
fn graph_reachability() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE e(a INT, b INT)", "INSERT INTO e VALUES (1,2),(1,3),(2,4),(3,4)"],
            "WITH RECURSIVE reach(n) AS (SELECT 1 UNION SELECT b FROM e JOIN reach ON e.a = reach.n) SELECT n FROM reach ORDER BY n",
            "reachability with UNION dedup on a diamond graph",
        ).await;
        agree(
            &["CREATE TABLE e(a INT, b INT)", "INSERT INTO e VALUES (1,2),(2,3),(3,4),(4,5)"],
            "WITH RECURSIVE reach(n) AS (SELECT 1 UNION SELECT b FROM e JOIN reach ON e.a = reach.n) SELECT count(*) FROM reach",
            "reachable node count on a chain",
        ).await;
    });
}

#[test]
fn outer_query_and_multi_cte() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 10) SELECT n FROM c WHERE n % 2 = 0 ORDER BY n",
              "outer-query filter over recursive result").await;
        agree(&[], "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 20) SELECT n FROM c ORDER BY n DESC LIMIT 3",
              "ORDER BY DESC + LIMIT over recursive result").await;
        agree(T, "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 6), named AS (SELECT id, name FROM tree) SELECT nums.n, named.name FROM nums JOIN named ON named.id = nums.n ORDER BY nums.n",
              "recursive CTE joined with a non-recursive CTE").await;
    });
}
