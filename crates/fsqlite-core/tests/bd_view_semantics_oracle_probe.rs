#![recursion_limit = "512"]

//! Regular-VIEW semantics leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! over non-INSTEAD-OF views — a view with explicit column aliases, a view over
//! a join, a view with an aggregate + GROUP BY, a nested view (view over view),
//! an ORDER BY inside the view body, a view referenced in a subquery / joined to
//! a base table, a WHERE filter applied on top of a view (predicate pushdown
//! must not change results), a view with a computed/expression column, and a
//! view selected with DISTINCT. Ordered result sets compared. Pass = keeper.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else { return vec![vec!["ERR".to_owned()]] };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect::<Vec<_>>())
    }) {
        Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn view_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)",
            "CREATE TABLE dept(name TEXT PRIMARY KEY, floor INTEGER)",
            "INSERT INTO emp VALUES (1,'Alice','eng',100),(2,'Bob','eng',90),(3,'Carol','sales',80),(4,'Dave','sales',120),(5,'Eve','ops',70)",
            "INSERT INTO dept VALUES ('eng',3),('sales',2),('ops',1)",
            // view with explicit column aliases + expression column
            "CREATE VIEW v_emp(eid, ename, dep, annual) AS SELECT id, name, dept, salary*12 FROM emp",
            // view over a join
            "CREATE VIEW v_join AS SELECT e.name AS ename, e.dept AS dep, d.floor AS flr FROM emp e JOIN dept d ON e.dept=d.name",
            // view with aggregate + GROUP BY
            "CREATE VIEW v_agg AS SELECT dept, count(*) AS n, sum(salary) AS total, max(salary) AS hi FROM emp GROUP BY dept",
            // view body with ORDER BY (SQLite keeps it; may be overridden by outer)
            "CREATE VIEW v_ordered AS SELECT id, salary FROM emp ORDER BY salary DESC",
            // nested view (view over v_agg)
            "CREATE VIEW v_nested AS SELECT dept, total FROM v_agg WHERE n > 1",
            // view with DISTINCT
            "CREATE VIEW v_depts AS SELECT DISTINCT dept FROM emp",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // aliased + expression columns
        check("view aliases+expr", fq(&f, "SELECT eid, ename, dep, annual FROM v_emp ORDER BY eid").await,
              rq(&r, "SELECT eid, ename, dep, annual FROM v_emp ORDER BY eid"), &mut diffs);
        // WHERE filter on top of the view (predicate pushdown)
        check("view where filter", fq(&f, "SELECT ename FROM v_emp WHERE dep='eng' AND annual > 1100 ORDER BY ename").await,
              rq(&r, "SELECT ename FROM v_emp WHERE dep='eng' AND annual > 1100 ORDER BY ename"), &mut diffs);
        // view over a join
        check("view join", fq(&f, "SELECT ename, flr FROM v_join ORDER BY ename").await,
              rq(&r, "SELECT ename, flr FROM v_join ORDER BY ename"), &mut diffs);
        // view with aggregate
        check("view aggregate", fq(&f, "SELECT dept, n, total, hi FROM v_agg ORDER BY dept").await,
              rq(&r, "SELECT dept, n, total, hi FROM v_agg ORDER BY dept"), &mut diffs);
        // aggregate view with an outer HAVING-like filter
        check("view agg outer filter", fq(&f, "SELECT dept FROM v_agg WHERE total >= 190 ORDER BY dept").await,
              rq(&r, "SELECT dept FROM v_agg WHERE total >= 190 ORDER BY dept"), &mut diffs);
        // ORDER BY inside the view, plus an outer LIMIT
        check("view inner orderby + outer limit", fq(&f, "SELECT id, salary FROM v_ordered LIMIT 3").await,
              rq(&r, "SELECT id, salary FROM v_ordered LIMIT 3"), &mut diffs);
        // outer ORDER BY overrides the inner one
        check("view outer orderby override", fq(&f, "SELECT id FROM v_ordered ORDER BY id").await,
              rq(&r, "SELECT id FROM v_ordered ORDER BY id"), &mut diffs);
        // nested view
        check("view nested", fq(&f, "SELECT dept, total FROM v_nested ORDER BY dept").await,
              rq(&r, "SELECT dept, total FROM v_nested ORDER BY dept"), &mut diffs);
        // DISTINCT view
        check("view distinct", fq(&f, "SELECT dept FROM v_depts ORDER BY dept").await,
              rq(&r, "SELECT dept FROM v_depts ORDER BY dept"), &mut diffs);
        // view joined to a base table
        check("view join base", fq(&f, "SELECT a.dept, a.total, d.floor FROM v_agg a JOIN dept d ON a.dept=d.name ORDER BY a.dept").await,
              rq(&r, "SELECT a.dept, a.total, d.floor FROM v_agg a JOIN dept d ON a.dept=d.name ORDER BY a.dept"), &mut diffs);
        // view in a scalar subquery
        check("view scalar subquery", fq(&f, "SELECT (SELECT max(total) FROM v_agg)").await,
              rq(&r, "SELECT (SELECT max(total) FROM v_agg)"), &mut diffs);
        // view in an IN subquery
        check("view in-subquery", fq(&f, "SELECT name FROM emp WHERE dept IN (SELECT dept FROM v_depts WHERE dept<>'ops') ORDER BY name").await,
              rq(&r, "SELECT name FROM emp WHERE dept IN (SELECT dept FROM v_depts WHERE dept<>'ops') ORDER BY name"), &mut diffs);
        // view appears in sqlite_master as type='view'
        check("view in schema", fq(&f, "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name").await,
              rq(&r, "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"), &mut diffs);
        // count over an aggregate view
        check("count over view", fq(&f, "SELECT count(*) FROM v_agg").await,
              rq(&r, "SELECT count(*) FROM v_agg"), &mut diffs);

        assert!(diffs.is_empty(), "{} VIEW-semantics divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
