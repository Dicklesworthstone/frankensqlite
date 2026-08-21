//! Differential oracle: JSON1 table-valued functions (json_each / json_tree)
//! and deep-path scalars vs rusqlite (bundled SQLite 3.53). A probe sweep found
//! the standalone/literal forms stock-correct across 12 cases (asserted here).
//!
//! KNOWN GAP (bd-tfwym): the *lateral* form `FROM t, json_each(t.col)` — a TVF
//! whose argument references a sibling FROM table — is unimplemented (frank
//! errors "column not found"). The two `#[ignore]`d tests below assert the stock
//! result and should be un-ignored once bd-tfwym lands.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
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
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
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
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

#[test]
fn json_each_standalone() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT key, value, type FROM json_each('[10,20,30]') ORDER BY key", "json_each over array").await;
        agree(&[], "SELECT key, value FROM json_each('{\"a\":1,\"b\":2,\"c\":3}') ORDER BY key", "json_each over object").await;
        agree(&[], "SELECT value FROM json_each('{\"arr\":[7,8,9]}', '$.arr') ORDER BY value", "json_each with path").await;
        agree(&[], "SELECT value, type FROM json_each('[1,\"s\",null,true,2.5]') ORDER BY id", "json_each value types").await;
    });
}

#[test]
fn json_tree_walk() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT type, count(*) FROM json_tree('{\"a\":[1,2],\"b\":{\"c\":3}}') GROUP BY type ORDER BY type", "json_tree node types").await;
        agree(&[], "SELECT fullkey, value FROM json_tree('{\"a\":[1,2],\"b\":3}') WHERE type='integer' ORDER BY fullkey", "json_tree fullkey leaves").await;
    });
}

#[test]
fn deep_paths_and_types() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT json_extract('{\"x\":1,\"y\":2,\"z\":3}', '$.x', '$.z')", "json_extract multi-path -> array").await;
        agree(&[], "SELECT json_type('{\"a\":[1,2]}','$.a'), json_type('{\"a\":[1,2]}','$.a[0]'), json_type('{\"o\":{}}','$.o')", "json_type at paths").await;
        agree(&[], "SELECT json_array_length('{\"a\":{\"b\":[1,2,3,4]}}', '$.a.b')", "json_array_length at path").await;
        agree(&[], "SELECT json_extract('{\"a\":{\"b\":{\"c\":[0,{\"d\":42}]}}}', '$.a.b.c[1].d')", "deep json_extract path").await;
        agree(&[], "SELECT '{\"a\":{\"b\":[10,20]}}' -> '$.a' ->> '$.b[1]'", "chained ->/->>").await;
        agree(&[], "SELECT json_group_array(value) FROM json_each('[3,1,2]')", "json_group_array over json_each (integrates bd-json-group-aggregates-pdvx5)").await;
    });
}

// ── Lateral TVF: KNOWN GAP bd-tfwym. Un-ignore when the lateral join lands. ──

#[test]
#[ignore = "bd-tfwym: lateral json_each(t.col) referencing a sibling FROM table is unimplemented"]
fn lateral_json_each_join() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT, tags TEXT)", "INSERT INTO t VALUES (1,'[\"x\",\"y\"]'),(2,'[\"y\",\"z\"]')"],
            "SELECT t.id, je.value FROM t, json_each(t.tags) je ORDER BY t.id, je.value",
            "lateral json_each over a column",
        ).await;
    });
}

#[test]
#[ignore = "bd-tfwym: lateral json_each(t.col) referencing a sibling FROM table is unimplemented"]
fn lateral_json_each_aggregate() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT, arr TEXT)", "INSERT INTO t VALUES (1,'[1,2,3]'),(2,'[9]')"],
            "SELECT t.id, count(*) FROM t, json_each(t.arr) GROUP BY t.id ORDER BY t.id",
            "lateral json_each feeding a per-row count",
        ).await;
    });
}
