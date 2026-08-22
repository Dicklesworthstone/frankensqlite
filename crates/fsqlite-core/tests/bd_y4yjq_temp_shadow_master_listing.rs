#![recursion_limit = "512"]

//! bd-y4yjq regression: after `CREATE TEMP TABLE t` shadows a same-named main
//! table, `main.sqlite_master` must STILL list the main `t` (the shadowed main
//! table is parked in shadowed_main_tables but remains a real main object).
//! Frank formerly omitted it (build_sqlite_master_rows only iterated self.schema,
//! where the temp entry took over the name and got filtered). Asserted against
//! the rusqlite oracle. Also checks sqlite_temp_master and cross-checks that the
//! main table stays directly queryable.

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
fn temp_shadow_keeps_main_in_sqlite_master() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE INDEX ix_t_name ON t(name)",
            "CREATE TABLE only_main(id INTEGER, v TEXT)",
            "INSERT INTO t VALUES (1,'main-a'),(2,'main-b')",
            "CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, tag TEXT)",  // shadows main.t
            "INSERT INTO temp.t VALUES (10,'temp10')",
            "CREATE TEMP TABLE scratch(x INTEGER)",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // THE FIX: main.sqlite_master lists the shadowed main table `t` + only_main
        check("main tables listed", fq(&f, "SELECT name FROM main.sqlite_master WHERE type='table' ORDER BY name").await,
              rq(&r, "SELECT name FROM main.sqlite_master WHERE type='table' ORDER BY name"), &mut diffs);
        // the main table's index is still listed with tbl_name='t'
        check("main index listed", fq(&f, "SELECT name,tbl_name FROM main.sqlite_master WHERE type='index' AND tbl_name='t' ORDER BY name").await,
              rq(&r, "SELECT name,tbl_name FROM main.sqlite_master WHERE type='index' AND tbl_name='t' ORDER BY name"), &mut diffs);
        // sqlite_temp_master lists the temp objects
        check("temp master", fq(&f, "SELECT name FROM sqlite_temp_master WHERE type='table' ORDER BY name").await,
              rq(&r, "SELECT name FROM sqlite_temp_master WHERE type='table' ORDER BY name"), &mut diffs);
        // the shadowed main table is still directly queryable via main.t
        check("main.t queryable", fq(&f, "SELECT id,name FROM main.t ORDER BY id").await,
              rq(&r, "SELECT id,name FROM main.t ORDER BY id"), &mut diffs);
        // unqualified t still resolves to the temp shadow
        check("unqualified -> temp", fq(&f, "SELECT id,tag FROM t ORDER BY id").await,
              rq(&r, "SELECT id,tag FROM t ORDER BY id"), &mut diffs);
        // total table count in main
        check("main table count", fq(&f, "SELECT count(*) FROM main.sqlite_master WHERE type='table'").await,
              rq(&r, "SELECT count(*) FROM main.sqlite_master WHERE type='table'"), &mut diffs);
        // after DROP of the temp shadow, main.sqlite_master still lists t exactly once
        ex(&f, &r, "DROP TABLE temp.t").await;
        check("after drop shadow", fq(&f, "SELECT name FROM main.sqlite_master WHERE type='table' ORDER BY name").await,
              rq(&r, "SELECT name FROM main.sqlite_master WHERE type='table' ORDER BY name"), &mut diffs);

        assert!(diffs.is_empty(), "{} bd-y4yjq divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
