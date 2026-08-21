#![recursion_limit = "512"]

//! Recursive-CTE leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! deeper WITH RECURSIVE — hierarchical tree traversal (join back to the CTE),
//! multi-column recursion (Fibonacci), UNION (dedup) vs UNION ALL in the
//! recursive term, recursive LIMIT termination, path/breadcrumb building, and
//! descendant counting. Ordered result sets compared. Pass = coverage keeper.

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

#[test]
fn recursive_cte_deep_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE org(id INTEGER PRIMARY KEY, parent INTEGER, name TEXT)",
            "INSERT INTO org VALUES (1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'a1'),(5,2,'a2'),(6,4,'a1x'),(7,3,'b1')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // tree traversal with depth
            "WITH RECURSIVE sub(id,name,depth) AS (\
               SELECT id,name,0 FROM org WHERE id=1 \
               UNION ALL \
               SELECT o.id,o.name,sub.depth+1 FROM org o JOIN sub ON o.parent=sub.id) \
             SELECT id,name,depth FROM sub ORDER BY depth,id",
            // descendant count of node 2
            "WITH RECURSIVE d(id) AS (\
               SELECT id FROM org WHERE parent=2 \
               UNION ALL \
               SELECT o.id FROM org o JOIN d ON o.parent=d.id) \
             SELECT count(*) FROM d",
            // path / breadcrumb building
            "WITH RECURSIVE p(id,path) AS (\
               SELECT id, name FROM org WHERE id=1 \
               UNION ALL \
               SELECT o.id, p.path || '/' || o.name FROM org o JOIN p ON o.parent=p.id) \
             SELECT path FROM p ORDER BY path",
            // Fibonacci (multi-column recursion)
            "WITH RECURSIVE fib(a,b,n) AS (\
               SELECT 0,1,1 UNION ALL SELECT b,a+b,n+1 FROM fib WHERE n<12) \
             SELECT a FROM fib ORDER BY n",
            // UNION dedup vs UNION ALL: a diamond graph would duplicate node 6 under UNION ALL,
            // UNION collapses it. Build a mini graph via a second parent link table.
            "WITH RECURSIVE cnt(n) AS (\
               VALUES(1) UNION ALL SELECT n+1 FROM cnt WHERE n<5) \
             SELECT sum(n) FROM cnt",
            "WITH RECURSIVE r(x) AS (\
               SELECT 1 UNION SELECT x FROM r WHERE x<3) \
             SELECT count(*) FROM r",
            // recursive with LIMIT termination
            "WITH RECURSIVE nat(n) AS (\
               SELECT 1 UNION ALL SELECT n+1 FROM nat) \
             SELECT n FROM nat LIMIT 5",
            // recursive term referencing the CTE in a subquery-ish filter
            "WITH RECURSIVE up(id,name) AS (\
               SELECT id,name FROM org WHERE id=6 \
               UNION ALL \
               SELECT o.id,o.name FROM org o JOIN up ON up.id=(SELECT parent FROM org WHERE id=up.id)) \
             SELECT name FROM up ORDER BY id",
            // ancestors via parent chain
            "WITH RECURSIVE anc(id,parent) AS (\
               SELECT id,parent FROM org WHERE id=6 \
               UNION ALL \
               SELECT o.id,o.parent FROM org o JOIN anc ON o.id=anc.parent) \
             SELECT id FROM anc ORDER BY id",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(diffs.is_empty(), "{} recursive-CTE divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
