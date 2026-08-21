#![recursion_limit = "512"]

//! Leaf-hunt differential (pane af49, 2026-08-20): frank vs rusqlite over the
//! table-valued json_each / json_tree functions — nested-JSON iteration with
//! key/value/type/atom/id/parent/fullkey/path columns, optional root path arg,
//! one-level (json_each) vs recursive (json_tree) traversal, and use in a real
//! FROM/WHERE/join context. Full ordered result sets compared (id/parent
//! numbering + traversal order are part of the contract). Distinct from the
//! scalar JSON sweep and from bd-76x57 (aggregate subtype). Pass = coverage
//! keeper; a mismatch is a leaf divergence.

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
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap();
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

#[test]
fn json_each_tree_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let queries = [
            "SELECT key, value, type, atom, fullkey, path FROM json_each('[10,20,30]')",
            "SELECT key, value, type, fullkey, path FROM json_each('{\"a\":1,\"b\":\"x\",\"c\":null}')",
            "SELECT key, value, type FROM json_each('{\"a\":{\"b\":1},\"c\":[2,3]}')",
            "SELECT key, value, type FROM json_each('{\"o\":{\"p\":9}}','$.o')",
            "SELECT value FROM json_each('[1,2,3,4]') WHERE value > 2",
            "SELECT sum(value) FROM json_each('[1,2,3,4,5]')",
            "SELECT count(*) FROM json_each('{\"a\":1,\"b\":2,\"c\":3}')",
            "SELECT type, count(*) FROM json_each('[1,\"x\",null,true,2.5]') GROUP BY type ORDER BY type",
            // json_tree — recursive traversal. NOTE: the id/parent columns are
            // documented opaque ("arbitrary and may change from one release to
            // the next"), so they are NOT compared across implementations —
            // frank uses a self-consistent sequential counter, stock uses
            // internal JSONB offsets; both satisfy the parent-references-id
            // contract. The well-defined columns (key/value/type/atom/fullkey/
            // path) are what must match.
            "SELECT key, value, type, atom, fullkey, path FROM json_tree('{\"a\":1,\"b\":[2,3]}')",
            "SELECT fullkey, type FROM json_tree('{\"x\":{\"y\":{\"z\":5}}}')",
            "SELECT count(*) FROM json_tree('{\"a\":[1,2,{\"b\":3}]}')",
            "SELECT value FROM json_tree('{\"a\":[1,2,{\"b\":3}]}') WHERE type='integer' ORDER BY value",
            "SELECT fullkey, atom FROM json_tree('[{\"k\":1},{\"k\":2}]') WHERE atom IS NOT NULL ORDER BY fullkey",
            // bd-kzwze: a PATH-rooted scan reports the self/root row's `key` as
            // the last path segment (object key -> TEXT, array index -> INTEGER),
            // NOT NULL. The `$`-rooted scans above still report root key NULL.
            "SELECT key, value FROM json_tree('{\"a\":{\"b\":1}}','$.a')",
            "SELECT key, value, type FROM json_tree('{\"outer\":{\"a\":1,\"b\":2}}','$.outer')",
            "SELECT key, value, type FROM json_tree('[10,[20,30]]','$[1]')",
            "SELECT key, value FROM json_tree('{\"a\":5}','$.a')",
            "SELECT key, value FROM json_each('{\"a\":5}','$.a')",
            "SELECT key, value, type FROM json_each('[10,[20,30]]','$[1]')",
            "SELECT key FROM json_tree('{\"a\":1}')",
            // atom is NULL for containers, non-NULL for leaves
            "SELECT type, atom FROM json_each('[[1],2,{\"k\":3}]') ORDER BY key",
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
            "{} json_each/json_tree divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
