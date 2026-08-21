#![recursion_limit = "512"]

//! bd-qear2 (increment 2): the core JSON read functions — json_extract,
//! json_type, json_each, json_tree — accept JSON5 input (unquoted keys, single
//! quotes, trailing commas, comments, hex) and behave as if it were the
//! canonicalized standard JSON, matching stock SQLite 3.42+. Non-finite
//! (+Infinity/-Infinity/NaN) is a documented follow-up and not asserted here.
//! Oracle = rusqlite.

use fsqlite_core::connection::{Connection, Row};
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

fn rows_f(rows: &[Row]) -> Vec<Vec<String>> {
    rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect()
}
async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows_f(&rows),
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
fn json5_read_functions_bd_qear2() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let queries = [
            // json_extract on JSON5
            "SELECT json_extract('{a:1}','$.a')",
            "SELECT json_extract('{a:{b:2}}','$.a.b')",
            "SELECT json_extract('{''x'':5}','$.x')",
            "SELECT json_extract('[1,2,3,]','$[1]')",
            "SELECT json_extract('{x:0xFF}','$.x')",
            "SELECT json_extract('[1, /* c */ 2]','$[1]')",
            "SELECT json_extract('{a:1,b:2}','$.b')",
            // json_type on JSON5
            "SELECT json_type('{a:1}')",
            "SELECT json_type('{a:1}','$.a')",
            "SELECT json_type('{''s'':''hi''}','$.s')",
            "SELECT json_type('[1,2,]','$[0]')",
            // json_each / json_tree on JSON5
            "SELECT key, value, type FROM json_each('{a:1, b:2,}')",
            "SELECT value FROM json_each('[10, 20, 30,]') WHERE value > 15",
            "SELECT fullkey, type FROM json_tree('{a:{b:1}}')",
            "SELECT count(*) FROM json_each('{x:0xA, y:0xB}')",
            // standard JSON still works (regression)
            "SELECT json_extract('{\"a\":9}','$.a')",
            "SELECT json_type('[1,2,3]')",
            // invalid under both
            "SELECT json_extract('{a:}','$.a')",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} JSON5 read-function divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
