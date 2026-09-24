#![recursion_limit = "512"]

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

/// GH#419: correlated `EXISTS` probes whose predicate is not a single
/// correlated equality (OR chains, expression operands) scan the probe table
/// directly instead of compiling a nested statement per outer row. Pin the
/// comparison semantics that path must preserve — column affinity on either
/// side, declared NOCASE vs explicit BINARY collation, INTEGER PRIMARY KEY
/// aliases, NULL outer values, `SELECT *` probes — plus a large indexed table
/// that must keep the seekable nested-statement path, both in plain SELECTs and
/// inside recursive CTE arms.
#[test]
fn gh419_correlated_exists_scan_probe_matches_sqlite() {
    const SETUP: &[&str] = &[
        "CREATE TABLE k(t TEXT, i INTEGER, r REAL, b)",
        "INSERT INTO k VALUES ('1', 1, 1.0, '1'), ('02', 2, 2.5, 2), ('abc', NULL, NULL, x'61'), (NULL, 4, 4.0, NULL)",
        "CREATE TABLE kc(name TEXT COLLATE NOCASE, tag TEXT)",
        "INSERT INTO kc VALUES ('Alpha', 'x'), ('beta ', 'y'), ('GAMMA', NULL)",
        "CREATE TABLE kp(id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO kp VALUES (3, 'c'), (7, 'g'), (10, 'j')",
        "CREATE TABLE big(a INTEGER, s TEXT)",
        "CREATE INDEX big_a ON big(a)",
        "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x < 200) INSERT INTO big SELECT x, 'v' || x FROM n",
        "CREATE TABLE o1(x, y TEXT, z INTEGER)",
        "INSERT INTO o1 VALUES (1,'1',1),(2,'2','2'),(3,'abc',3),(4,NULL,4),('1',1,'1'),(2.5,'2.5',2.5),(NULL,NULL,NULL),(x'61','a',97)",
        "CREATE TABLE o2(w TEXT, u)",
        "INSERT INTO o2 VALUES ('alpha','alpha'),('BETA','BETA'),('beta ','beta '),('gamma','x'),('delta',NULL),('A','A'),('b','b')",
    ];
    const CASES: &[(&str, &str)] = &[
        (
            "SELECT x, y, z FROM o1 WHERE NOT EXISTS (SELECT 1 FROM k WHERE k.t = o1.x OR k.i = o1.y) ORDER BY rowid",
            "NOT EXISTS, TEXT/INTEGER inner columns vs untyped/TEXT outer",
        ),
        (
            "SELECT x, y, z FROM o1 WHERE EXISTS (SELECT 1 FROM k WHERE k.r = o1.x OR k.b = o1.z) ORDER BY rowid",
            "EXISTS, REAL and untyped inner columns",
        ),
        (
            "SELECT x, y, z FROM o1 WHERE EXISTS (SELECT 1 FROM k WHERE o1.y = k.i OR o1.z = k.t OR o1.x = k.b) ORDER BY rowid",
            "outer operand on the left of each comparison",
        ),
        (
            "SELECT w FROM o2 WHERE EXISTS (SELECT 1 FROM kc WHERE kc.name = o2.w OR kc.tag = o2.w) ORDER BY rowid",
            "declared NOCASE inner column on the left",
        ),
        (
            "SELECT w FROM o2 WHERE NOT EXISTS (SELECT 1 FROM kc WHERE o2.w = kc.name OR o2.u = kc.tag) ORDER BY rowid",
            "outer BINARY column on the left of a NOCASE column",
        ),
        (
            "SELECT u FROM o2 WHERE EXISTS (SELECT 1 FROM kc WHERE o2.u = kc.name OR o2.u || '' = kc.name) ORDER BY rowid",
            "untyped outer column and expression vs NOCASE column",
        ),
        (
            "SELECT w FROM o2 WHERE EXISTS (SELECT 1 FROM kc WHERE kc.name = o2.w COLLATE BINARY OR substr(kc.name, 1, 1) = o2.w) ORDER BY rowid",
            "explicit COLLATE BINARY overrides the declared NOCASE",
        ),
        (
            "SELECT w FROM o2 WHERE EXISTS (SELECT 1 FROM kc AS q WHERE q.name = o2.w AND q.tag IS NOT NULL OR q.name LIKE o2.u) ORDER BY rowid",
            "aliased probe table, AND inside OR, LIKE",
        ),
        (
            "SELECT x, z FROM o1 WHERE EXISTS (SELECT 1 FROM kp WHERE kp.id = o1.z OR v = char(o1.z + 96)) ORDER BY rowid",
            "INTEGER PRIMARY KEY alias read from the rowid",
        ),
        (
            "SELECT x FROM o1 WHERE NOT EXISTS (SELECT * FROM k WHERE k.i = o1.x OR (k.i IS o1.x AND k.t IS NULL)) ORDER BY rowid",
            "SELECT * probe and NULL outer values",
        ),
        (
            "SELECT x, z FROM o1 WHERE EXISTS (SELECT 1 FROM big WHERE big.a = o1.z OR big.s = 'v' || (o1.z + 100)) ORDER BY rowid",
            "large indexed probe table keeps the nested-statement path",
        ),
        (
            "WITH RECURSIVE c(n, path) AS (SELECT 1, '1' UNION ALL SELECT n+1, path || ',' || (n+1) FROM c WHERE n < 12 AND NOT EXISTS (SELECT 1 FROM kp WHERE kp.id = c.n + 1 OR kp.v = substr('abcdefghijkl', c.n + 1, 1))) SELECT n, path FROM c ORDER BY n",
            "recursive arm stops when the probe first matches",
        ),
        (
            "WITH RECURSIVE c(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM c, kc WHERE n < 3 AND EXISTS (SELECT 1 FROM kc AS z WHERE z.name = kc.tag OR z.tag = kc.tag)) SELECT n, count(*) FROM c GROUP BY n ORDER BY n",
            "recursive arm joined with the probed table under an alias",
        ),
        (
            "WITH RECURSIVE c(n, s) AS (SELECT 1, 'x' UNION ALL SELECT n+1, s || n FROM c, o2 WHERE n < 3 AND NOT EXISTS (SELECT 1 FROM kc WHERE kc.name = o2.w OR kc.tag = substr(c.s, n, 1))) SELECT n, s, count(*) FROM c GROUP BY n, s ORDER BY n, s",
            "probe correlated with both the working table and a joined table",
        ),
    ];
    asupersync::test_utils::run_test(|| async {
        for (sql, msg) in CASES {
            agree(SETUP, sql, msg).await;
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
