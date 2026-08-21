#![recursion_limit = "512"]

//! bd-elre4: jsonb_extract() returns JSONB bytes ONLY where json_extract would
//! return a JSON array/object; a single path resolving to a JSON scalar (or a
//! missing path -> NULL) returns the SQL scalar value, same as json_extract.
//! frank previously JSONB-encoded scalars too. Oracle = rusqlite; container
//! results are compared via a json() round-trip (JSONB bytes are opaque).

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

async fn fval(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) if rows.len() == 1 => tag_f(&rows[0].values()[0]),
        Ok(rows) => format!("ROWS:{}", rows.len()),
        Err(_) => "ERR".to_owned(),
    }
}
fn rval(conn: &rusqlite::Connection, sql: &str) -> String {
    match conn.query_row(sql, [], |row| {
        Ok(tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)))
    }) {
        Ok(s) => s,
        Err(_) => "ERR".to_owned(),
    }
}

#[test]
fn jsonb_extract_scalar_vs_container_bd_elre4() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // single path -> scalar => SQL scalar (NOT JSONB bytes)
            "SELECT jsonb_extract(jsonb('{\"a\":1}'),'$.a')",
            "SELECT jsonb_extract(jsonb('{\"a\":\"hi\"}'),'$.a')",
            "SELECT jsonb_extract(jsonb('{\"a\":1.5}'),'$.a')",
            "SELECT jsonb_extract(jsonb('{\"a\":true}'),'$.a')",
            "SELECT jsonb_extract(jsonb('{\"a\":false}'),'$.a')",
            "SELECT jsonb_extract(jsonb('{\"a\":null}'),'$.a')",
            "SELECT jsonb_extract(jsonb('[10,20,30]'),'$[1]')",
            "SELECT typeof(jsonb_extract(jsonb('{\"a\":1}'),'$.a'))",
            "SELECT typeof(jsonb_extract(jsonb('{\"a\":1.5}'),'$.a'))",
            // missing single path -> NULL
            "SELECT jsonb_extract(jsonb('{\"a\":1}'),'$.b')",
            "SELECT jsonb_extract(jsonb('{\"a\":1}'),'$.a.b')",
            // container -> JSONB, round-tripped via json()
            "SELECT json(jsonb_extract(jsonb('{\"a\":{\"b\":2}}'),'$.a'))",
            "SELECT json(jsonb_extract(jsonb('{\"a\":[1,2,3]}'),'$.a'))",
            "SELECT typeof(jsonb_extract(jsonb('{\"a\":[1]}'),'$.a'))",
            // multi-path -> JSONB array, round-tripped
            "SELECT json(jsonb_extract(jsonb('{\"a\":1,\"b\":2}'),'$.a','$.b'))",
            "SELECT typeof(jsonb_extract(jsonb('{\"a\":1,\"b\":2}'),'$.a','$.b'))",
            // deep scalar
            "SELECT jsonb_extract(jsonb('{\"a\":[{\"b\":7}]}'),'$.a[0].b')",
        ];

        let mut diffs = Vec::new();
        for e in exprs {
            let fv = fval(&f, e).await;
            let rv = rval(&r, e);
            if fv != rv {
                diffs.push(format!("  `{e}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} jsonb_extract scalar/container divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
