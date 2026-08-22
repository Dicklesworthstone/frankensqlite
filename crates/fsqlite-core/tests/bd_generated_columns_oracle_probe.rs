#![recursion_limit = "512"]

//! Generated-column leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! VIRTUAL and STORED generated columns — computation from base columns,
//! recompute on UPDATE, use in WHERE / ORDER BY / aggregates, indexes on a
//! generated column, typeof, text/arithmetic/CASE expressions, and rejection of
//! an explicit value for a generated column. Final state compared. Pass =
//! coverage keeper; a mismatch is a leaf divergence.

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
fn generated_columns_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        for s in [
            "CREATE TABLE t(\
               a INT, b INT,\
               s INT GENERATED ALWAYS AS (a + b) STORED,\
               v AS (a * b) VIRTUAL,\
               lbl TEXT GENERATED ALWAYS AS (a || '/' || b),\
               sgn TEXT AS (CASE WHEN a < 0 THEN 'neg' WHEN a = 0 THEN 'zero' ELSE 'pos' END) VIRTUAL)",
            "CREATE INDEX iv ON t(v)",
            "INSERT INTO t(a,b) VALUES (2,3),(5,1),(-4,10),(0,7)",
        ] { ex(&f, &r, s).await; }

        check("computed values", fq(&f, "SELECT a,b,s,v,lbl,sgn FROM t ORDER BY a").await, rq(&r, "SELECT a,b,s,v,lbl,sgn FROM t ORDER BY a"), &mut diffs);
        check("typeof gen", fq(&f, "SELECT typeof(s), typeof(v), typeof(lbl) FROM t WHERE a=2").await, rq(&r, "SELECT typeof(s), typeof(v), typeof(lbl) FROM t WHERE a=2"), &mut diffs);

        // recompute on UPDATE of a base column
        ex(&f, &r, "UPDATE t SET a = a + 100 WHERE b = 3").await;
        check("recompute on update", fq(&f, "SELECT a,b,s,v,lbl FROM t WHERE b=3").await, rq(&r, "SELECT a,b,s,v,lbl FROM t WHERE b=3"), &mut diffs);

        // use generated columns in WHERE / ORDER BY / aggregates / index
        check("where on stored", fq(&f, "SELECT a FROM t WHERE s > 6 ORDER BY a").await, rq(&r, "SELECT a FROM t WHERE s > 6 ORDER BY a"), &mut diffs);
        check("order by virtual", fq(&f, "SELECT a,v FROM t ORDER BY v, a").await, rq(&r, "SELECT a,v FROM t ORDER BY v, a"), &mut diffs);
        check("agg over generated", fq(&f, "SELECT sum(s), max(v), group_concat(lbl,'|') FROM t").await, rq(&r, "SELECT sum(s), max(v), group_concat(lbl,'|') FROM t"), &mut diffs);
        check("index lookup on virtual", fq(&f, "SELECT a FROM t WHERE v = 5").await, rq(&r, "SELECT a FROM t WHERE v = 5"), &mut diffs);

        // explicit value for a generated column must be rejected -> no row added
        ex(&f, &r, "INSERT INTO t(a,b,s) VALUES (1,1,999)").await;
        check("reject explicit gen value", fq(&f, "SELECT count(*) FROM t").await, rq(&r, "SELECT count(*) FROM t"), &mut diffs);

        assert!(diffs.is_empty(), "{} generated-column divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
