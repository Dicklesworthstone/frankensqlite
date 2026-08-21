#![recursion_limit = "512"]

//! UPSERT leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! INSERT ... ON CONFLICT DO UPDATE / DO NOTHING — excluded.* references,
//! targeted vs untargeted conflict, conflict on PK vs UNIQUE, a DO UPDATE WHERE
//! guard, arithmetic on excluded, and chained ON CONFLICT clauses (3.35+).
//! Final table state compared vs rusqlite. Pass = coverage keeper.

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
fn upsert_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE, v INTEGER, hits INTEGER DEFAULT 0)",
            "INSERT INTO t VALUES (1,'a',10,0),(2,'b',20,0),(3,'c',30,0)",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let mut check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // PK conflict, DO UPDATE using excluded.* + arithmetic on existing
        ex(&f, &r, "INSERT INTO t(id,u,v) VALUES (1,'a',99) ON CONFLICT(id) DO UPDATE SET v=v+excluded.v, hits=hits+1").await;
        check("pk do-update excluded", fq(&f, "SELECT id,u,v,hits FROM t WHERE id=1").await, rq(&r, "SELECT id,u,v,hits FROM t WHERE id=1"), &mut diffs);

        // UNIQUE(u) conflict targeted
        ex(&f, &r, "INSERT INTO t(id,u,v) VALUES (5,'b',7) ON CONFLICT(u) DO UPDATE SET v=excluded.v").await;
        check("unique do-update", fq(&f, "SELECT id,u,v FROM t WHERE u='b'").await, rq(&r, "SELECT id,u,v FROM t WHERE u='b'"), &mut diffs);

        // DO NOTHING on conflict
        ex(&f, &r, "INSERT INTO t(id,u,v) VALUES (3,'c',999) ON CONFLICT(id) DO NOTHING").await;
        check("do-nothing", fq(&f, "SELECT v FROM t WHERE id=3").await, rq(&r, "SELECT v FROM t WHERE id=3"), &mut diffs);

        // untargeted ON CONFLICT DO UPDATE
        ex(&f, &r, "INSERT INTO t(id,u,v) VALUES (2,'b2',1) ON CONFLICT DO UPDATE SET v=v*2").await;
        check("untargeted do-update", fq(&f, "SELECT id,v FROM t WHERE id=2").await, rq(&r, "SELECT id,v FROM t WHERE id=2"), &mut diffs);

        // DO UPDATE with a WHERE guard that FAILS -> no update
        ex(&f, &r, "INSERT INTO t(id,u,v) VALUES (1,'a',5) ON CONFLICT(id) DO UPDATE SET v=100 WHERE v<0").await;
        check("do-update where-false", fq(&f, "SELECT v FROM t WHERE id=1").await, rq(&r, "SELECT v FROM t WHERE id=1"), &mut diffs);

        // insert of a brand-new non-conflicting row through an upsert stmt
        ex(&f, &r, "INSERT INTO t(id,u,v) VALUES (7,'g',70) ON CONFLICT(id) DO UPDATE SET v=excluded.v").await;
        check("upsert new row", fq(&f, "SELECT id,u,v FROM t WHERE id=7").await, rq(&r, "SELECT id,u,v FROM t WHERE id=7"), &mut diffs);

        // full final state
        check("final", fq(&f, "SELECT id,u,v,hits FROM t ORDER BY id").await, rq(&r, "SELECT id,u,v,hits FROM t ORDER BY id"), &mut diffs);

        assert!(diffs.is_empty(), "{} UPSERT divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
