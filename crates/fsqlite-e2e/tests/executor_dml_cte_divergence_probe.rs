//! Executor / DML / CTE oracle DIVERGENCE PROBE (not a keeper): exercise the
//! complex query executor — multi-way joins, CTEs (incl. recursive), compound
//! selects, subquery correlation, and mutating DML (INSERT..SELECT, UPSERT,
//! UPDATE..FROM, DELETE..WHERE EXISTS, RETURNING) — against C SQLite (rusqlite),
//! printing every scenario whose setup-error parity OR final result set diverges.
//! Complements the scalar and aggregate/window/TVF probes with the higher-
//! complexity surface where divergences are likeliest to still hide.
//!
//! Each scenario runs on a FRESH frank + rusqlite pair (DML is stateful), so
//! scenarios are isolated. The `verify` query's result set is compared as a
//! SORTED multiset (row order is not asserted unless the query has ORDER BY, in
//! which case sorting still agrees). A setup statement that errors on exactly one
//! engine is itself a divergence.
//!
//! `#[ignore]` by default; run with:
//!   cargo test -p fsqlite-e2e --test executor_dml_cte_divergence_probe -- --ignored --nocapture
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i:{n}"),
        SqliteValue::Float(f) => format!("f:{f}"),
        SqliteValue::Text(s) => format!("t:{s}"),
        SqliteValue::Blob(b) => format!("b:{}", b.len()),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<String>, String> {
    match conn.query(sql).await {
        Ok(rs) => {
            let mut rows: Vec<String> = rs
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(render_frank)
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect();
            rows.sort();
            Ok(rows)
        }
        Err(e) => Err(e.to_string()),
    }
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => return Err(format!("prep: {e}")),
    };
    let n = stmt.column_count();
    let out = stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(n);
        for i in 0..n {
            let cell = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                rusqlite::types::ValueRef::Integer(x) => format!("i:{x}"),
                rusqlite::types::ValueRef::Real(f) => format!("f:{f}"),
                rusqlite::types::ValueRef::Text(t) => format!("t:{}", String::from_utf8_lossy(t)),
                rusqlite::types::ValueRef::Blob(b) => format!("b:{}", b.len()),
            };
            cells.push(cell);
        }
        Ok(cells.join(","))
    });
    match out {
        Ok(iter) => {
            let collected: Result<Vec<String>, _> = iter.collect();
            match collected {
                Ok(mut rows) => {
                    rows.sort();
                    Ok(rows)
                }
                Err(e) => Err(format!("run: {e}")),
            }
        }
        Err(e) => Err(format!("map: {e}")),
    }
}

/// (name, setup statements run on both engines, final verify query).
type Scenario = (&'static str, &'static [&'static str], &'static str);

const BASE: &[&str] = &[
    "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept INTEGER, salary INTEGER)",
    "CREATE TABLE dept(id INTEGER PRIMARY KEY, dname TEXT)",
    "INSERT INTO dept VALUES (1,'eng'),(2,'sales'),(3,'ops')",
    "INSERT INTO emp(name,dept,salary) VALUES \
     ('ann',1,100),('bob',1,120),('cal',2,90),('dan',2,110),('eve',NULL,80)",
];

fn scenarios() -> Vec<Scenario> {
    vec![
        // ---- joins ----
        (
            "inner_join",
            BASE,
            "SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept=d.id",
        ),
        (
            "left_join_nulls",
            BASE,
            "SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept=d.id",
        ),
        (
            "left_join_where_right",
            BASE,
            "SELECT e.name FROM emp e LEFT JOIN dept d ON e.dept=d.id WHERE d.dname='eng'",
        ),
        (
            "cross_join",
            BASE,
            "SELECT count(*) FROM emp CROSS JOIN dept",
        ),
        (
            "self_join",
            BASE,
            "SELECT a.name, b.name FROM emp a JOIN emp b ON a.dept=b.dept AND a.id<b.id",
        ),
        (
            "join_using",
            BASE,
            "SELECT count(*) FROM emp JOIN dept ON emp.dept=dept.id",
        ),
        (
            "three_way",
            BASE,
            "SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept=d.id JOIN emp e2 ON e2.dept=e.dept AND e2.id=e.id",
        ),
        (
            "left_join_coalesce",
            BASE,
            "SELECT e.name, COALESCE(d.dname,'none') FROM emp e LEFT JOIN dept d ON e.dept=d.id",
        ),
        (
            "join_agg_group",
            BASE,
            "SELECT d.dname, count(e.id), sum(e.salary) FROM dept d LEFT JOIN emp e ON e.dept=d.id GROUP BY d.dname",
        ),
        (
            "dept_with_no_emp",
            BASE,
            "SELECT d.dname FROM dept d LEFT JOIN emp e ON e.dept=d.id WHERE e.id IS NULL",
        ),
        // ---- CTEs ----
        (
            "cte_basic",
            BASE,
            "WITH high AS (SELECT * FROM emp WHERE salary>=100) SELECT name FROM high",
        ),
        (
            "cte_multi",
            BASE,
            "WITH a AS (SELECT id FROM emp WHERE dept=1), b AS (SELECT id FROM emp WHERE dept=2) SELECT count(*) FROM (SELECT * FROM a UNION ALL SELECT * FROM b)",
        ),
        (
            "cte_referenced_twice",
            BASE,
            "WITH c AS (SELECT dept, avg(salary) av FROM emp GROUP BY dept) SELECT c1.dept FROM c c1 JOIN c c2 ON c1.av>=c2.av GROUP BY c1.dept HAVING count(*)=(SELECT count(*) FROM c)",
        ),
        (
            "recursive_counter",
            BASE,
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<5) SELECT n FROM seq",
        ),
        (
            "recursive_fib",
            BASE,
            "WITH RECURSIVE fib(a,b) AS (SELECT 0,1 UNION ALL SELECT b,a+b FROM fib WHERE b<50) SELECT a FROM fib",
        ),
        (
            "recursive_limit",
            BASE,
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq) SELECT n FROM seq LIMIT 6",
        ),
        (
            "recursive_sum",
            BASE,
            "WITH RECURSIVE seq(n,s) AS (SELECT 1,1 UNION ALL SELECT n+1,s+n+1 FROM seq WHERE n<5) SELECT n,s FROM seq",
        ),
        // ---- compound selects ----
        (
            "union",
            BASE,
            "SELECT dept FROM emp UNION SELECT id FROM dept",
        ),
        (
            "union_all",
            BASE,
            "SELECT count(*) FROM (SELECT dept FROM emp UNION ALL SELECT id FROM dept)",
        ),
        (
            "intersect",
            BASE,
            "SELECT dept FROM emp INTERSECT SELECT id FROM dept",
        ),
        (
            "except",
            BASE,
            "SELECT id FROM dept EXCEPT SELECT dept FROM emp",
        ),
        (
            "values_clause",
            BASE,
            "SELECT column1, column2 FROM (VALUES (1,'x'),(2,'y'),(3,'z'))",
        ),
        (
            "compound_orderby_limit",
            BASE,
            "SELECT salary FROM emp UNION SELECT salary+1 FROM emp ORDER BY salary DESC LIMIT 3",
        ),
        // ---- subqueries ----
        (
            "scalar_subq",
            BASE,
            "SELECT name, salary-(SELECT avg(salary) FROM emp) FROM emp WHERE dept=1",
        ),
        (
            "correlated_where",
            BASE,
            "SELECT name FROM emp e WHERE salary=(SELECT max(salary) FROM emp e2 WHERE e2.dept=e.dept)",
        ),
        (
            "exists",
            BASE,
            "SELECT dname FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept=d.id)",
        ),
        (
            "not_exists",
            BASE,
            "SELECT dname FROM dept d WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept=d.id)",
        ),
        (
            "in_subq",
            BASE,
            "SELECT name FROM emp WHERE dept IN (SELECT id FROM dept WHERE dname IN ('eng','ops'))",
        ),
        (
            "derived_table",
            BASE,
            "SELECT d, mx FROM (SELECT dept d, max(salary) mx FROM emp GROUP BY dept) WHERE mx>=100",
        ),
        // ---- DML: INSERT variants (verify by SELECT of final state) ----
        (
            "insert_select",
            &[
                "CREATE TABLE t(a INTEGER, b TEXT)",
                "CREATE TABLE s(a INTEGER, b TEXT)",
                "INSERT INTO s VALUES (1,'x'),(2,'y')",
                "INSERT INTO t SELECT a*10, upper(b) FROM s",
            ],
            "SELECT a,b FROM t",
        ),
        (
            "insert_or_ignore_pk",
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
                "INSERT INTO t VALUES (1,'x')",
                "INSERT OR IGNORE INTO t VALUES (1,'y'),(2,'z')",
            ],
            "SELECT a,b FROM t",
        ),
        (
            "insert_or_replace_pk",
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
                "INSERT INTO t VALUES (1,'x')",
                "INSERT OR REPLACE INTO t VALUES (1,'y'),(2,'z')",
            ],
            "SELECT a,b FROM t",
        ),
        (
            "upsert_do_nothing",
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
                "INSERT INTO t VALUES (1,'x')",
                "INSERT INTO t VALUES (1,'y') ON CONFLICT(a) DO NOTHING",
            ],
            "SELECT a,b FROM t",
        ),
        (
            "upsert_do_update",
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, n INTEGER DEFAULT 0)",
                "INSERT INTO t VALUES (1,'x',5)",
                "INSERT INTO t(a,b) VALUES (1,'y') ON CONFLICT(a) DO UPDATE SET b=excluded.b, n=n+1",
            ],
            "SELECT a,b,n FROM t",
        ),
        (
            "upsert_unique",
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, u TEXT UNIQUE, c INTEGER)",
                "INSERT INTO t VALUES (1,'k',1)",
                "INSERT INTO t VALUES (2,'k',9) ON CONFLICT(u) DO UPDATE SET c=c+excluded.c",
            ],
            "SELECT a,u,c FROM t",
        ),
        // ---- DML: UPDATE / DELETE ----
        ("update_where", BASE, "SELECT id FROM emp WHERE 0"),
        (
            "update_from",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TABLE d(id INTEGER, add_v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "INSERT INTO d VALUES (1,5),(3,7)",
                "UPDATE t SET v=v+d.add_v FROM d WHERE t.id=d.id",
            ],
            "SELECT id,v FROM t",
        ),
        (
            "update_subq_set",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "UPDATE t SET v=(SELECT max(v) FROM t) WHERE id=1",
            ],
            "SELECT id,v FROM t",
        ),
        (
            "delete_where_exists",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER)",
                "CREATE TABLE keep(g INTEGER)",
                "INSERT INTO t VALUES (1,1),(2,2),(3,3)",
                "INSERT INTO keep VALUES (2)",
                "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM keep WHERE keep.g=t.g)",
            ],
            "SELECT id,g FROM t",
        ),
        (
            "delete_in_subq",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)",
                "DELETE FROM t WHERE v IN (SELECT v FROM t WHERE v>25)",
            ],
            "SELECT id FROM t",
        ),
        // ---- DML: RETURNING (verify = the RETURNING rows themselves) ----
        (
            "insert_returning",
            &["CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)"],
            "INSERT INTO t(b) VALUES ('x'),('y') RETURNING a, b",
        ),
        (
            "update_returning",
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
            ],
            "UPDATE t SET v=v*2 RETURNING a, v",
        ),
        (
            "delete_returning",
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
            ],
            "DELETE FROM t WHERE v>=20 RETURNING a",
        ),
        // ---- views / generated ----
        (
            "view_select",
            &[
                "CREATE TABLE t(a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1,2),(3,4),(5,6)",
                "CREATE VIEW v AS SELECT a+b AS s FROM t WHERE a>1",
            ],
            "SELECT s FROM v",
        ),
        (
            "generated_col",
            &[
                "CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a+b) VIRTUAL)",
                "INSERT INTO t(a,b) VALUES (1,2),(10,20)",
            ],
            "SELECT a,b,c FROM t",
        ),
    ]
}

#[test]
#[ignore = "executor/DML/CTE divergence probe (not a keeper): frank-vs-sqlite3 mismatches"]
fn executor_dml_cte_divergence_probe() {
    asupersync::test_utils::run_test(|| async {
        let mut diverged = 0usize;
        let mut both_err = 0usize;
        for (name, setup, verify) in scenarios() {
            let f = Connection::open(":memory:").await.expect("open frank");
            let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
            // Run setup; a per-statement error-parity mismatch is a divergence.
            let mut setup_diverged = false;
            for s in setup {
                let fe = f.execute(s).await;
                let re = r.execute_batch(s);
                if fe.is_ok() != re.is_ok() {
                    diverged += 1;
                    setup_diverged = true;
                    println!(
                        "SETUP-DIVERGE [{name}] `{s}`\n    frank: {:?}\n    csql:  {:?}",
                        fe.as_ref()
                            .map(|_| "ok")
                            .map_err(std::string::ToString::to_string),
                        re.as_ref().map(|_| "ok").map_err(|e| e.to_string())
                    );
                    break;
                }
            }
            if setup_diverged {
                continue;
            }
            let fr = frank_rows(&f, verify).await;
            let sr = sqlite_rows(&r, verify);
            match (&fr, &sr) {
                (Ok(a), Ok(b)) => {
                    if a != b {
                        diverged += 1;
                        println!("DIVERGE [{name}] {verify}\n    frank: {a:?}\n    csql:  {b:?}");
                    }
                }
                (Err(_), Err(_)) => both_err += 1,
                (Ok(a), Err(b)) => {
                    diverged += 1;
                    println!(
                        "F-OK/C-ERR [{name}] {verify}\n    frank: {a:?}\n    csql:  <err: {b}>"
                    );
                }
                (Err(a), Ok(b)) => {
                    diverged += 1;
                    println!(
                        "F-ERR/C-OK [{name}] {verify}\n    frank: <err: {a}>\n    csql:  {b:?}"
                    );
                }
            }
        }
        println!(
            "\nPROBE SUMMARY: {} scenarios, {} diverged, {} both-error",
            scenarios().len(),
            diverged,
            both_err
        );
    });
}
